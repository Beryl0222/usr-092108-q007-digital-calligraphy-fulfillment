"""限量发行履约应用服务。

所有命令遵循同一套纪律：
1. 幂等：每个外部命令/回调带 request_id（causation_id），重放不产生新事件；
2. 原子：在事件存储全局锁内重放最新状态再决策，跨聚合联动一次提交；
3. 只追加：任何纠正都产出后继事件，不改写历史；
4. 守恒：占号/释放/配号均围绕「款式序号」这一唯一事实源。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from . import domain as d
from . import events as ev
from .errors import (
    DomainError, IllegalTransition, LatePaymentRejected, NotEligible, NotFound,
    PhysicalMismatch, SoldOut, AlreadyOccupiesQuota,
)
from .events import Event, EventIdPolicy, utc_now
from .store import EventStore

CHANNEL_FREE = d.KIND_FREE
CHANNEL_PAID = d.KIND_PAID

DEFAULT_HOLD_MINUTES = 15


class FulfilmentService:
    def __init__(self, store: EventStore, clock: Any = utc_now) -> None:
        self.store = store
        self.clock = clock
        self.ids = EventIdPolicy()

    # ==================================================================
    # 基础工具
    # ==================================================================

    def _state(self, agg_type: str, agg_id: str):
        events = self.store.stream(agg_id)
        return d.replay(agg_type, agg_id, events)

    def _make(self, etype: str, agg_type: str, agg_id: str, version: int,
              summary: str, payload: dict, request_id: str | None,
              correlation_id: str | None = None) -> Event:
        return Event(
            event_id=self.ids.new(agg_id, etype, request_id),
            event_type=etype, aggregate_type=agg_type, aggregate_id=agg_id,
            occurred_at=self.clock(), version=version, summary=summary,
            payload=payload, causation_id=request_id, correlation_id=correlation_id)

    def _commit(self, request_id: str | None, build) -> list[Event]:
        if request_id:
            prior = self.store.by_causation(request_id)
            if prior is not None:
                return [prior]  # 同一请求重放：原样返回首次结果
        return self.store.atomically(build)

    # ==================================================================
    # 出版项目与款式
    # ==================================================================

    def create_project(self, project_id: str, title: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            if self.store.version(project_id):
                return []  # 幂等：已建
            e = self._make(ev.PROJECT_CREATED, ev.AGG_PROJECT, project_id, 1,
                           f"出版项目《{title}》建档",
                           {"title": title}, request_id)
            return self.store.append([e], {project_id: 0})
        return self._commit(request_id, build)

    def open_variant(self, variant_id: str, project_id: str, title: str, kind: str,
                     edition_size: int, physical_capacity: int = 0,
                     requires_eligibility: bool = False,
                     request_id: str = "") -> list[Event]:
        if edition_size <= 0:
            raise DomainError("发行量必须为正整数")
        if kind == d.KIND_BUNDLE and physical_capacity == 0:
            physical_capacity = edition_size

        def build() -> list[Event]:
            if self.store.version(variant_id):
                return []
            payload = {"project_id": project_id, "title": title, "kind": kind,
                       "edition_size": edition_size,
                       "physical_capacity": physical_capacity,
                       "requires_eligibility": requires_eligibility}
            e1 = self._make(ev.VARIANT_OPENED, ev.AGG_VARIANT, variant_id, 1,
                            f"开放款式《{title}》，发行量 {edition_size}",
                            payload, request_id)
            events = [e1]
            expected = {variant_id: 0}
            # 项目侧同步登记款式（项目不存在则顺手建档）
            pv = self.store.version(project_id)
            if pv == 0:
                events.append(self._make(ev.PROJECT_CREATED, ev.AGG_PROJECT,
                                         project_id, 1, "项目随款式自动建档",
                                         {"title": project_id}, request_id + ":project"
                                         if request_id else None))
                expected[project_id] = 0
            else:
                events.append(self._make(ev.VARIANT_OPENED, ev.AGG_PROJECT,
                                         project_id, pv + 1,
                                         f"项目登记款式 {variant_id}",
                                         {"variant_id": variant_id}, request_id + ":project"
                                         if request_id else None))
                expected[project_id] = pv
            return self.store.append(events, expected)
        return self._commit(request_id, build)

    def close_variant(self, variant_id: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            v = self.store.version(variant_id)
            state = self._state(ev.AGG_VARIANT, variant_id)
            if state is None or state.status == d.VARIANT_CLOSED:
                return []
            e = self._make(ev.VARIANT_CLOSED, ev.AGG_VARIANT, variant_id, v + 1,
                           f"关闭款式 {variant_id}", {"reason": "发行结束"}, request_id)
            return self.store.append([e], {variant_id: v})
        return self._commit(request_id, build)

    # ==================================================================
    # 资格（免费与付费分别核验）
    # ==================================================================

    @staticmethod
    def eligibility_key(variant_id: str, user_id: str) -> str:
        return f"elig:{variant_id}:{user_id}"

    def grant_eligibility(self, variant_id: str, user_id: str, grant_kind: str,
                          channel: str, verified: bool = True,
                          request_id: str = "") -> list[Event]:
        key = self.eligibility_key(variant_id, user_id)

        def build() -> list[Event]:
            v = self.store.version(key)
            state = d.replay_eligibility(key, self.store.stream(key))
            if state is not None and state.active and state.grant_kind == grant_kind:
                return []
            e = self._make(ev.ELIGIBILITY_GRANTED, ev.AGG_ELIGIBILITY, key, v + 1,
                           f"授予用户 {user_id} 款式 {variant_id} 的"
                           f"{'免费领取' if grant_kind == d.KIND_FREE else '购买'}资格",
                           {"variant_id": variant_id, "user_id": user_id,
                            "grant_kind": grant_kind, "channel": channel,
                            "verified": verified}, request_id)
            return self.store.append([e], {key: v})
        return self._commit(request_id, build)

    def revoke_eligibility(self, variant_id: str, user_id: str, reason: str,
                           request_id: str) -> list[Event]:
        key = self.eligibility_key(variant_id, user_id)

        def build() -> list[Event]:
            v = self.store.version(key)
            state = d.replay_eligibility(key, self.store.stream(key))
            if state is None or not state.active:
                return []
            e = self._make(ev.ELIGIBILITY_REVOKED, ev.AGG_ELIGIBILITY, key, v + 1,
                           f"撤销用户 {user_id} 的资格：{reason}",
                           {"variant_id": variant_id, "user_id": user_id, "reason": reason},
                           request_id)
            return self.store.append([e], {key: v})
        return self._commit(request_id, build)

    def _check_eligibility(self, variant: d.VariantState, user_id: str, channel: str) -> None:
        key = self.eligibility_key(variant.variant_id, user_id)
        state = d.replay_eligibility(key, self.store.stream(key))
        if channel == d.KIND_FREE:
            # 免费领取：必须持有已验证的免费资格
            if state is None or not state.active or not state.verified \
                    or state.grant_kind != d.KIND_FREE:
                raise NotEligible(f"用户 {user_id} 无有效免费领取资格")
        else:
            # 付费购买：款式开启了资格门槛时，必须持有购买资格
            if variant.requires_eligibility:
                if state is None or not state.active or state.grant_kind != d.KIND_PAID:
                    raise NotEligible(f"用户 {user_id} 无该款式购买资格")

    # ==================================================================
    # 下单保留（并发只能占一个名额）
    # ==================================================================

    def place_order(self, order_id: str, variant_id: str, user_id: str, channel: str,
                    price: int = 0, hold_minutes: int = DEFAULT_HOLD_MINUTES,
                    physical_required: bool | None = None,
                    request_id: str = "") -> list[Event]:
        def build() -> list[Event]:
            existing = self.store.by_causation(request_id) if request_id else None
            if existing is not None:
                return [existing]

            variant = self._state(ev.AGG_VARIANT, variant_id)
            if variant is None:
                raise NotFound(f"款式不存在：{variant_id}")
            if variant.status != d.VARIANT_OPEN:
                raise IllegalTransition("款式未开放或已关闭")
            if channel == d.KIND_FREE and price not in (0, None):
                raise DomainError("免费领取价格必须为 0")

            # 已存在的订单：同单重试幂等返回；不同单重复领取拒绝。
            order_events = self.store.stream(order_id)
            if order_events:
                return []  # 订单号已占用，调用方应以 request_id 做重试
            self._check_eligibility(variant, user_id, channel)

            # 一人一名额（持有中或已购都算）
            if variant.serial_of_user(user_id) is not None:
                raise AlreadyOccupiesQuota(
                    f"用户 {user_id} 在款式 {variant_id} 已占用或拥有一个名额")

            free = variant.free_serials()
            if not free:
                raise SoldOut(f"款式 {variant_id} 已售罄/无可用保留名额")
            serial = min(free)  # 确定的最小可用序号，杜绝并发挑号撞号

            now = self.clock()
            expires = now + timedelta(minutes=hold_minutes)
            need_physical = variant.kind == d.KIND_BUNDLE if physical_required is None \
                else physical_required

            vv = self.store.version(variant_id)
            ov = self.store.version(order_id)
            corr = order_id
            events: list[Event] = []
            expected: dict[str, int] = {variant_id: vv, order_id: ov}
            if channel == d.KIND_FREE:
                # 免费领取：资格即支付，占号同时成交，无待支付保留期
                e_hold = self._make(ev.SERIAL_HELD, ev.AGG_VARIANT, variant_id, vv + 1,
                                    f"款式 {variant_id} 序号 {serial} 由订单 {order_id} 免费领取",
                                    {"variant_id": variant_id, "serial": serial,
                                     "order_id": order_id, "user_id": user_id,
                                     "expires_at": expires.isoformat(),
                                     "reason": "免费领取"}, request_id,
                                    correlation_id=corr)
                e_sold = self._make(ev.PAYMENT_CONFIRMED, ev.AGG_VARIANT, variant_id, vv + 2,
                                    f"序号 {serial} 免费领取成交给订单 {order_id}",
                                    {"variant_id": variant_id, "serial": serial,
                                     "order_id": order_id, "user_id": user_id,
                                     "payment_ref": "FREE_CLAIM"},
                                    request_id + ":sold" if request_id else None,
                                    correlation_id=corr)
                e_order = self._make(ev.ORDER_PLACED, ev.AGG_ORDER, order_id, 1,
                                     f"订单 {order_id} 免费领取下单",
                                     {"order_id": order_id,
                                      "project_id": variant.project_id,
                                      "variant_id": variant_id, "user_id": user_id,
                                      "channel": channel, "price": 0, "serial": serial,
                                      "expires_at": expires.isoformat(),
                                      "physical_required": need_physical},
                                     request_id + ":place" if request_id else None,
                                     correlation_id=corr)
                e_paid = self._make(ev.PAYMENT_CONFIRMED, ev.AGG_ORDER, order_id, 2,
                                    f"订单 {order_id} 免费领取即时成交",
                                    {"order_id": order_id, "variant_id": variant_id,
                                     "serial": serial, "user_id": user_id,
                                     "payment_ref": "FREE_CLAIM"},
                                    request_id + ":free" if request_id else None,
                                    correlation_id=corr)
                events[:] = [e_hold, e_sold, e_order, e_paid]
            else:
                e_hold = self._make(ev.SERIAL_HELD, ev.AGG_VARIANT, variant_id, vv + 1,
                                    f"款式 {variant_id} 序号 {serial} 为订单 {order_id} 保留",
                                    {"variant_id": variant_id, "serial": serial,
                                     "order_id": order_id, "user_id": user_id,
                                     "expires_at": expires.isoformat(),
                                     "reason": "下单保留"}, request_id,
                                    correlation_id=corr)
                e_order = self._make(ev.ORDER_PLACED, ev.AGG_ORDER, order_id, 1,
                                     f"订单 {order_id} 付费下单",
                                     {"order_id": order_id,
                                      "project_id": variant.project_id,
                                      "variant_id": variant_id, "user_id": user_id,
                                      "channel": channel, "price": price, "serial": serial,
                                      "expires_at": expires.isoformat(),
                                      "physical_required": need_physical},
                                     request_id + ":place" if request_id else None,
                                     correlation_id=corr)
                events[:] = [e_hold, e_order]
            return self.store.append(events, expected)
        return self._commit(request_id, build)

    def extend_hold(self, order_id: str, minutes: int, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if not o.is_held:
                raise IllegalTransition("仅待支付订单可延长保留")
            v = self.store.version(order_id)
            expires = self.clock() + timedelta(minutes=minutes)
            e = self._make(ev.HOLD_EXTENDED, ev.AGG_ORDER, order_id, v + 1,
                           f"订单 {order_id} 保留延长 {minutes} 分钟",
                           {"expires_at": expires.isoformat()}, request_id,
                           correlation_id=order_id)
            return self.store.append([e], {order_id: v})
        return self._commit(request_id, build)

    # ==================================================================
    # 支付回调（迟到支付不能再次成交）
    # ==================================================================

    def confirm_payment(self, order_id: str, payment_ref: str,
                        request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if payment_ref in o.payment_refs:
                return []  # 同一笔支付回调重放，幂等
            v = self.store.version(order_id)
            if o.status == d.ORDER_PAID:
                return []
            if o.status in (d.ORDER_CANCELLED, d.ORDER_REFUNDED):
                # 名额已释放/已退款：迟到支付只能登记拒绝事实，绝不再次成交。
                e = self._make(ev.PAYMENT_LATE_REJECTED, ev.AGG_ORDER, order_id, v + 1,
                               f"订单 {order_id} 的迟到支付 {payment_ref} 不予成交",
                               {"order_id": order_id, "variant_id": o.variant_id,
                                "serial": o.serial, "payment_ref": payment_ref,
                                "order_status": o.status}, request_id,
                               correlation_id=order_id)
                self.store.append([e], {order_id: v})
                raise LatePaymentRejected(order_id, payment_ref)
            if o.status != d.ORDER_HELD:
                raise IllegalTransition(f"订单状态 {o.status} 不能确认支付")

            vv = self.store.version(o.variant_id)
            e_pay = self._make(ev.PAYMENT_CONFIRMED, ev.AGG_ORDER, order_id, v + 1,
                               f"订单 {order_id} 支付确认 {payment_ref}",
                               {"order_id": order_id, "variant_id": o.variant_id,
                                "serial": o.serial, "user_id": o.user_id,
                                "payment_ref": payment_ref}, request_id,
                               correlation_id=order_id)
            e_sold = self._make(ev.PAYMENT_CONFIRMED, ev.AGG_VARIANT, o.variant_id, vv + 1,
                                f"序号 {o.serial} 正式售出给订单 {order_id}",
                                {"variant_id": o.variant_id, "serial": o.serial,
                                 "order_id": order_id, "user_id": o.user_id,
                                 "payment_ref": payment_ref},
                                request_id + ":sold" if request_id else None,
                                correlation_id=order_id)
            return self.store.append([e_pay, e_sold],
                                     {order_id: v, o.variant_id: vv})
        return self._commit(request_id, build)

    # ==================================================================
    # 超时释放 / 取消
    # ==================================================================

    def expire_holds(self, now: datetime | None = None) -> list[Event]:
        """释放所有到期未支付的保留。与迟到支付竞争时，先提交者生效。"""
        now = now or self.clock()
        results: list[Event] = []
        for order_id in self.store.aggregate_ids(ev.AGG_ORDER):
            o = self._state(ev.AGG_ORDER, order_id)
            if o is not None and o.is_held and o.expires_at is not None \
                    and o.expires_at <= now:
                results.extend(self._release(order_id, d.CANCEL_TIMEOUT, None))
        return results

    def cancel_order(self, order_id: str, request_id: str,
                     reason: str = d.CANCEL_USER) -> list[Event]:
        return self._release(order_id, reason, request_id)

    def _release(self, order_id: str, reason: str, request_id: str | None) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if o.status in (d.ORDER_CANCELLED, d.ORDER_REFUNDED):
                return []
            if o.status == d.ORDER_PAID:
                raise IllegalTransition("已支付订单请走退款/仲裁流程，不能直接释放")

            events: list[Event] = []
            expected: dict[str, int] = {}
            vv = self.store.version(o.variant_id)
            variant = self._state(ev.AGG_VARIANT, o.variant_id)
            if variant is not None and o.serial in variant.serial_status:
                events.append(self._make(ev.HOLD_RELEASED, ev.AGG_VARIANT, o.variant_id,
                                         vv + 1,
                                         f"序号 {o.serial} 释放（{reason}），名额可再发放",
                                         {"variant_id": o.variant_id, "serial": o.serial,
                                          "order_id": order_id, "reason": reason},
                                         request_id, correlation_id=order_id))
                expected[o.variant_id] = vv
            ov = self.store.version(order_id)
            events.append(self._make(ev.ORDER_CANCELLED, ev.AGG_ORDER, order_id, ov + 1,
                                     f"订单 {order_id} 取消：{reason}",
                                     {"order_id": order_id, "variant_id": o.variant_id,
                                      "serial": o.serial, "reason": reason},
                                     request_id + ":cancel" if request_id else None,
                                     correlation_id=order_id))
            expected[order_id] = ov
            # 已配实体但未发出：随撤销隔离，绝不发给别人
            fid = self._fulfilment_id(order_id)
            fstate = self._state(ev.AGG_FULFILLMENT, fid)
            if fstate is not None and fstate.status == d.PHY_ALLOCATED:
                fv = self.store.version(fid)
                events.append(self._make(ev.PHYSICAL_QUARANTINED, ev.AGG_FULFILLMENT,
                                         fid, fv + 1,
                                         f"订单 {order_id} 撤销，未发实体 {fstate.physical_unit} 隔离",
                                         {"order_id": order_id, "variant_id": o.variant_id,
                                          "serial": o.serial, "reason": reason,
                                          "physical_unit": fstate.physical_unit},
                                         request_id + ":quarantine" if request_id else None,
                                         correlation_id=order_id))
                expected[fid] = fv
            return self.store.append(events, expected)
        return self._commit(request_id, build)

    # ==================================================================
    # 链上登记（追加式，不可删除，纠正只能追加）
    # ==================================================================

    @staticmethod
    def _ledger_id(variant_id: str) -> str:
        return f"ledger:{variant_id}"

    def request_registration(self, order_id: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if o.status != d.ORDER_PAID:
                raise IllegalTransition("仅已支付订单可请求链上登记")
            lid = self._ledger_id(o.variant_id)
            ledger = self._state(ev.AGG_REGISTRY, lid)
            if ledger is not None and o.serial in ledger.records:
                return []  # 幂等：该序号已提交过登记请求
            lv = self.store.version(lid)
            ov = self.store.version(order_id)
            e = self._make(ev.REGISTRATION_REQUESTED, ev.AGG_REGISTRY, lid, lv + 1,
                           f"订单 {order_id} 序号 {o.serial} 请求链上登记",
                           {"variant_id": o.variant_id, "serial": o.serial,
                            "order_id": order_id, "user_id": o.user_id},
                           request_id, correlation_id=order_id)
            e2 = self._make(ev.REGISTRATION_REQUESTED, ev.AGG_ORDER, order_id, ov + 1,
                            f"订单 {order_id} 登记请求已发起",
                            {"variant_id": o.variant_id, "serial": o.serial,
                             "order_id": order_id, "request_id": request_id},
                            request_id + ":ord" if request_id else None,
                            correlation_id=order_id)
            return self.store.append([e, e2], {lid: lv, order_id: ov})
        return self._commit(request_id, build)

    def registration_outcome(self, order_id: str, accepted: bool, chain_ref: str | None,
                             reason: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            lid = self._ledger_id(o.variant_id)
            lv = self.store.version(lid)
            ledger = self._state(ev.AGG_REGISTRY, lid)
            etype = ev.REGISTRATION_ACCEPTED if accepted else ev.REGISTRATION_FAILED
            if ledger is not None:
                rec = ledger.records.get(o.serial)
                if rec is not None and rec.status == (d.REG_ACCEPTED if accepted else d.REG_FAILED):
                    return []
            payload = {"variant_id": o.variant_id, "serial": o.serial,
                       "order_id": order_id}
            summary = f"序号 {o.serial} 链上登记成立" if accepted else \
                f"序号 {o.serial} 链上登记失败：{reason}"
            if accepted:
                payload["chain_ref"] = chain_ref
            else:
                payload["reason"] = reason
            e = self._make(etype, ev.AGG_REGISTRY, lid, lv + 1, summary, payload,
                           request_id, correlation_id=order_id)
            return self.store.append([e], {lid: lv})
        return self._commit(request_id, build)

    def correct_registration(self, variant_id: str, serial: int, order_id: str,
                             new_status: str, note: str, request_id: str) -> list[Event]:
        """对不可删除的登记做追加式状态纠正（如争议后标注撤销/迁移）。"""
        lid = self._ledger_id(variant_id)

        def build() -> list[Event]:
            lv = self.store.version(lid)
            e = self._make(ev.REGISTRATION_CORRECTED, ev.AGG_REGISTRY, lid, lv + 1,
                           f"序号 {serial} 登记追加纠正：{new_status}（原记录保留）",
                           {"variant_id": variant_id, "serial": serial,
                            "order_id": order_id, "new_status": new_status, "note": note},
                           request_id, correlation_id=order_id)
            return self.store.append([e], {lid: lv})
        return self._commit(request_id, build)

    # ==================================================================
    # 实体仓配：配号必须等于数字收藏序号；补发沿用原号
    # ==================================================================

    @staticmethod
    def _fulfilment_id(order_id: str) -> str:
        return f"F-{order_id}"

    def allocate_physical(self, order_id: str, physical_unit: str,
                          request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if not o.physical_required:
                raise IllegalTransition("该订单不含实体件")
            if o.status != d.ORDER_PAID:
                raise IllegalTransition("仅已支付订单可配实体件")
            fid = self._fulfilment_id(order_id)
            fv = self.store.version(fid)
            if fv:
                return []  # 已配货，幂等
            e = self._make(ev.PHYSICAL_ALLOCATED, ev.AGG_FULFILLMENT, fid, 1,
                           f"订单 {order_id} 实体件按收藏序号 {o.serial} 配号",
                           {"order_id": order_id, "variant_id": o.variant_id,
                            "serial": o.serial, "physical_unit": physical_unit},
                           request_id, correlation_id=order_id)
            return self.store.append([e], {fid: 0})
        return self._commit(request_id, build)

    def dispatch_physical(self, order_id: str, tracking_no: str, carrier: str,
                          request_id: str) -> list[Event]:
        def build() -> list[Event]:
            f = self._state(ev.AGG_FULFILLMENT, self._fulfilment_id(order_id))
            if f is None:
                raise NotFound("实体履约单不存在，请先配货")
            if f.status not in (d.PHY_ALLOCATED,):
                raise IllegalTransition(f"实体状态 {f.status} 不能首次发货")
            fv = self.store.version(f.fulfillment_id)
            e = self._make(ev.PHYSICAL_DISPATCHED, ev.AGG_FULFILLMENT,
                           f.fulfillment_id, fv + 1,
                           f"订单 {order_id} 实体发出，运单 {tracking_no}",
                           {"order_id": order_id, "variant_id": f.variant_id,
                            "serial": f.serial, "tracking_no": tracking_no,
                            "carrier": carrier, "kind": "original"},
                           request_id, correlation_id=order_id)
            return self.store.append([e], {f.fulfillment_id: fv})
        return self._commit(request_id, build)

    def deliver_physical(self, order_id: str, request_id: str) -> list[Event]:
        return self._physical_event(order_id, ev.PHYSICAL_DELIVERED,
                                    {d.PHY_DISPATCHED}, "签收", request_id)

    def report_lost(self, order_id: str, reason: str, request_id: str) -> list[Event]:
        return self._physical_event(
            order_id, ev.PHYSICAL_LOST, {d.PHY_DISPATCHED},
            f"物流判丢：{reason}", request_id, extra_payload={"reason": reason})

    def reship_lost(self, order_id: str, tracking_no: str, carrier: str,
                    request_id: str) -> list[Event]:
        """丢件补发：沿用原收藏序号配号，不产生新编号。"""
        return self._reship(order_id, tracking_no, carrier, request_id,
                            reason="丢件补发")

    def reship_returned(self, order_id: str, tracking_no: str, carrier: str,
                        request_id: str) -> list[Event]:
        """拒收/退回后经仲裁确认的重发：同样沿用原收藏序号。"""
        return self._reship(order_id, tracking_no, carrier, request_id,
                            reason="退回后按仲裁结论重发")

    def _reship(self, order_id: str, tracking_no: str, carrier: str,
                request_id: str, reason: str) -> list[Event]:
        def build() -> list[Event]:
            fid = self._fulfilment_id(order_id)
            f = self._state(ev.AGG_FULFILLMENT, fid)
            if f is None:
                raise NotFound("实体履约单不存在")
            if f.status not in (d.PHY_LOST, d.PHY_RETURNED):
                raise IllegalTransition(f"实体状态 {f.status} 不允许补发/重发")
            o = self._state(ev.AGG_ORDER, order_id)
            if o is not None and f.serial != o.serial:
                # 补发配号必须与收藏序号一致——理论上不该发生，是最后防线
                raise PhysicalMismatch(
                    f"补发配号 {f.serial} 与收藏序号 {o.serial} 不一致")
            fv = self.store.version(fid)
            e = self._make(ev.PHYSICAL_RESENT, ev.AGG_FULFILLMENT, fid, fv + 1,
                           f"订单 {order_id} {reason}，沿用收藏序号 {f.serial}，"
                           f"新运单 {tracking_no}",
                           {"order_id": order_id, "variant_id": f.variant_id,
                            "serial": f.serial, "tracking_no": tracking_no,
                            "carrier": carrier,
                            "reship_reason": f.status,
                            "original_tracking":
                                f.shipments[-1].tracking_no if f.shipments else None},
                           request_id, correlation_id=order_id)
            return self.store.append([e], {fid: fv})
        return self._commit(request_id, build)

    def report_returned(self, order_id: str, reason: str, request_id: str) -> list[Event]:
        return self._physical_event(order_id, ev.PHYSICAL_RETURNED,
                                    {d.PHY_DISPATCHED, d.PHY_DELIVERED},
                                    f"实体退回：{reason}", request_id,
                                    extra_payload={"reason": reason})

    def _physical_event(self, order_id: str, etype: str, allowed: set[str],
                        summary: str, request_id: str,
                        extra_payload: dict | None = None) -> list[Event]:
        def build() -> list[Event]:
            fid = self._fulfilment_id(order_id)
            f = self._state(ev.AGG_FULFILLMENT, fid)
            if f is None:
                raise NotFound("实体履约单不存在")
            if f.status not in allowed:
                raise IllegalTransition(f"实体状态 {f.status} 不允许 {etype}")
            fv = self.store.version(fid)
            payload = {"order_id": order_id, "variant_id": f.variant_id,
                       "serial": f.serial}
            if extra_payload:
                payload.update(extra_payload)
            e = self._make(etype, ev.AGG_FULFILLMENT, fid, fv + 1,
                           f"订单 {order_id} {summary}", payload, request_id,
                           correlation_id=order_id)
            return self.store.append([e], {fid: fv})
        return self._commit(request_id, build)

    # ==================================================================
    # 退款（未发实体隔离；悬空状态由仲裁闭环）
    # ==================================================================

    def grant_refund(self, order_id: str, reason: str, request_id: str,
                     revoke_digital: bool = False,
                     correction_note: str = "") -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if o.status == d.ORDER_REFUNDED:
                return []
            if o.status != d.ORDER_PAID:
                raise IllegalTransition(f"订单状态 {o.status} 不能退款")

            events: list[Event] = []
            expected: dict[str, int] = {}
            ov = self.store.version(order_id)
            events.append(self._make(ev.REFUND_GRANTED, ev.AGG_ORDER, order_id, ov + 1,
                                     f"订单 {order_id} 退款：{reason}",
                                     {"order_id": order_id, "variant_id": o.variant_id,
                                      "serial": o.serial, "reason": reason,
                                      "revoke_digital": revoke_digital},
                                     request_id, correlation_id=order_id))
            expected[order_id] = ov

            # 未发实体：随撤销隔离；已发出的走退回/补发，不允许静默回收。
            fid = self._fulfilment_id(order_id)
            f = self._state(ev.AGG_FULFILLMENT, fid)
            if f is not None and f.status in (d.PHY_ALLOCATED,):
                fv = self.store.version(fid)
                events.append(self._make(ev.PHYSICAL_QUARANTINED, ev.AGG_FULFILLMENT,
                                         fid, fv + 1,
                                         f"订单 {order_id} 退款，未发实体隔离",
                                         {"order_id": order_id, "variant_id": o.variant_id,
                                          "serial": o.serial, "reason": reason,
                                          "physical_unit": f.physical_unit},
                                         request_id + ":quarantine" if request_id else None,
                                         correlation_id=order_id))
                expected[fid] = fv

            # 数字登记不可删除：需要撤销时以追加状态纠正留痕。
            if revoke_digital:
                lid = self._ledger_id(o.variant_id)
                ledger = self._state(ev.AGG_REGISTRY, lid)
                if ledger is not None and o.serial in ledger.records:
                    lv = self.store.version(lid)
                    events.append(self._make(
                        ev.REGISTRATION_CORRECTED, ev.AGG_REGISTRY, lid, lv + 1,
                        f"序号 {o.serial} 退款后追加状态纠正（原登记保留）",
                        {"variant_id": o.variant_id, "serial": o.serial,
                         "order_id": order_id, "new_status": "revoked",
                         "note": correction_note or reason},
                        request_id + ":correction" if request_id else None,
                        correlation_id=order_id))
                    expected[lid] = lv
            return self.store.append(events, expected)
        return self._commit(request_id, build)

    # ==================================================================
    # 发票
    # ==================================================================

    def request_invoice(self, invoice_id: str, order_id: str, title: str, tax_no: str,
                        request_id: str) -> list[Event]:
        def build() -> list[Event]:
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            if self.store.version(invoice_id):
                return []
            e = self._make(ev.INVOICE_REQUESTED, ev.AGG_INVOICE, invoice_id, 1,
                           f"订单 {order_id} 申请发票",
                           {"order_id": order_id, "title": title, "tax_no": tax_no,
                            "amount": o.price}, request_id, correlation_id=order_id)
            return self.store.append([e], {invoice_id: 0})
        return self._commit(request_id, build)

    def issue_invoice(self, invoice_id: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            inv = self._state(ev.AGG_INVOICE, invoice_id)
            if inv is None:
                raise NotFound("发票不存在")
            if inv.status == d.INVOICE_ISSUED:
                return []
            v = self.store.version(invoice_id)
            e = self._make(ev.INVOICE_ISSUED, ev.AGG_INVOICE, invoice_id, v + 1,
                           f"发票 {invoice_id} 开具",
                           {"order_id": inv.order_id}, request_id,
                           correlation_id=inv.order_id)
            return self.store.append([e], {invoice_id: v})
        return self._commit(request_id, build)

    def void_invoice(self, invoice_id: str, request_id: str) -> list[Event]:
        def build() -> list[Event]:
            inv = self._state(ev.AGG_INVOICE, invoice_id)
            if inv is None:
                raise NotFound("发票不存在")
            if inv.status == d.INVOICE_VOIDED:
                return []
            v = self.store.version(invoice_id)
            e = self._make(ev.INVOICE_VOIDED, ev.AGG_INVOICE, invoice_id, v + 1,
                           f"发票 {invoice_id} 作废（原票据保留）",
                           {"order_id": inv.order_id}, request_id,
                           correlation_id=inv.order_id)
            return self.store.append([e], {invoice_id: v})
        return self._commit(request_id, build)

    def reissue_invoice(self, credit_id: str, original_invoice_id: str,
                        request_id: str) -> list[Event]:
        """退款后红冲/贷记凭证：追加，不删除原发票。"""
        def build() -> list[Event]:
            inv = self._state(ev.AGG_INVOICE, original_invoice_id)
            if inv is None:
                raise NotFound("原发票不存在")
            if self.store.version(credit_id):
                return []
            e = self._make(ev.INVOICE_REISSUED, ev.AGG_INVOICE, credit_id, 1,
                           f"针对原发票 {original_invoice_id} 开具贷记凭证",
                           {"order_id": inv.order_id,
                            "related_invoice_id": original_invoice_id,
                            "kind": d.INVOICE_CREDIT, "amount": -inv.amount},
                           request_id, correlation_id=inv.order_id)
            return self.store.append([e], {credit_id: 0})
        return self._commit(request_id, build)

    # ==================================================================
    # 仲裁 / 客服工单
    # ==================================================================

    def open_arbitration(self, case_id: str, order_id: str, reason: str,
                         request_id: str) -> list[Event]:
        def build() -> list[Event]:
            if self.store.version(case_id):
                return []
            o = self._state(ev.AGG_ORDER, order_id)
            if o is None:
                raise NotFound(f"订单不存在：{order_id}")
            e = self._make(ev.ARBITRATION_OPENED, ev.AGG_CASE, case_id, 1,
                           f"订单 {order_id} 进入仲裁：{reason}",
                           {"order_id": order_id, "reason": reason,
                            "variant_id": o.variant_id, "serial": o.serial},
                           request_id, correlation_id=order_id)
            return self.store.append([e], {case_id: 0})
        return self._commit(request_id, build)

    def resolve_arbitration(self, case_id: str, resolution: str, note: str,
                            request_id: str) -> list[Event]:
        def build() -> list[Event]:
            c = self._state(ev.AGG_CASE, case_id)
            if c is None:
                raise NotFound("仲裁案件不存在")
            v = self.store.version(case_id)
            e = self._make(ev.ARBITRATION_RESOLVED, ev.AGG_CASE, case_id, v + 1,
                           f"案件 {case_id} 仲裁结论：{resolution}",
                           {"order_id": c.order_id, "resolution": resolution,
                            "note": note}, request_id, correlation_id=c.order_id)
            return self.store.append([e], {case_id: v})
        return self._commit(request_id, build)


# LatePaymentRejected 从 errors 导出，保持本模块路径也可引用
__all__ = ["FulfilmentService", "LatePaymentRejected", "CHANNEL_FREE", "CHANNEL_PAID",
           "DEFAULT_HOLD_MINUTES"]
