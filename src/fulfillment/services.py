"""限量发行履约领域服务。

每个命令都在事件存储的同一把锁内完成「折叠现状 → 业务裁决 → 原子追加」，
因此：

* 并发下单不可能拿到同一序号，也不可能超过款式上限；
* 超时释放与迟到支付无论以什么次序到达，订单都只会有一个结局；
* 外部回调以请求标识（causation_id）去重，重复送达返回首次结果。

所有更正都表现为新事件：登记不删除、只追加纠正；未发出的实体件随撤销
进入隔离号段；丢件补发沿用原收藏序号。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import model
from .envelope import envelope
from .errors import Conflict, DomainError, EligibilityRefused, NotFound, SoldOut
from .event_store import EventStore

# ---- 稳定的错误码，便于调用方分支 -----------------------------------------
LATE_PAYMENT_REJECTED = "LATE_PAYMENT_REJECTED"
ALREADY_SETTLED = "ALREADY_SETTLED"


def _uid() -> str:
    return uuid.uuid4().hex[:12]


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


class FulfillmentService:
    def __init__(self, store: EventStore, *, hold_ttl_seconds: int = 15 * 60,
                 clock: Callable[[], datetime] | None = None) -> None:
        self.store = store
        self.hold_ttl = timedelta(seconds=hold_ttl_seconds)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return self._clock()

    # ======================================================================
    # 一、出版项目与款式配置
    # ======================================================================

    def publish_project(self, project_id: str, name: str, *, physical_cap: int = 1000) -> None:
        def tx() -> None:
            if self.store.version_of("project", project_id):
                raise Conflict(f"项目已存在：{project_id}")
            self.store.append(
                envelope(
                    "PROJECT_PUBLISHED", "project", project_id, 1,
                    f"出版项目 {name} 立项，实体组合上限 {physical_cap} 套",
                    {"name": name, "physical_cap": physical_cap,
                     "physical_pool_id": f"POOL-PHYS-{project_id}"},
                ),
                expected_version=0,
            )
        self.store.transaction(tx)

    def configure_variant(self, project_id: str, variant_id: str, name: str, *,
                          kind: str, cap: int = 1000, price: int = 0) -> None:
        def tx() -> None:
            project = self._load_project(project_id)
            if variant_id in project.variants:
                raise Conflict(f"款式已存在：{variant_id}")
            if kind not in ("digital_paid", "digital_free"):
                raise DomainError("kind 必须是 digital_paid 或 digital_free")
            if kind == "digital_free" and price != 0:
                raise DomainError("免费款式定价必须为 0")
            if kind == "digital_paid" and price <= 0:
                raise DomainError("付费款式定价必须为正数")
            pool_id = f"POOL-{variant_id}"
            # 款式与序号池共用配置事件的两侧投影：事件挂在 edition 聚合，
            # 池台账在首次占用时按 cap 建立。
            self.store.append(
                envelope(
                    "VARIANT_CONFIGURED", "edition", variant_id, 1,
                    f"款式 {name} 配置上限 {cap} 份",
                    {"project_id": project_id, "name": name, "kind": kind,
                     "cap": cap, "price": price, "pool_id": pool_id},
                ),
                expected_version=0,
            )
        self.store.transaction(tx)

    def publish_claim_rule(self, project_id: str, *, free_variant_ids: list[str],
                           one_free_per_user: bool = True) -> None:
        def tx() -> None:
            project = self._load_project(project_id)
            for vid in free_variant_ids:
                if vid not in project.variants:
                    raise NotFound(f"免费款式未配置：{vid}")
            version = self.store.version_of("project", project_id)
            self.store.append(
                envelope(
                    "CLAIM_RULE_PUBLISHED", "project", project_id, version + 1,
                    "发布领取资格规则",
                    {"free_variant_ids": free_variant_ids, "one_free_per_user": one_free_per_user},
                ),
                expected_version=version,
            )
        self.store.transaction(tx)

    # ======================================================================
    # 二、用户资格（免费凭领取资格，付费凭购买资格，分别核验）
    # ======================================================================

    def grant_entitlement(self, user_id: str, scope: str, ref: str, *,
                          grant_id: str | None = None, note: str = "") -> str:
        """登记一份免费领取资格。scope 为项目或款式标识。"""
        ent_id = grant_id or f"ENT-{user_id}-{scope}-{ref}"

        def tx() -> str:
            existing = self.store.events_for("entitlement", ent_id)
            if existing:
                return ent_id  # 资格授予天然幂等
            self.store.append(
                envelope(
                    "ENTITLEMENT_GRANTED", "entitlement", ent_id, 1,
                    f"授予用户 {user_id} 领取资格（{scope}/{ref}）",
                    {"user_id": user_id, "scope": scope, "ref": ref, "note": note},
                ),
                expected_version=0,
            )
            return ent_id
        return self.store.transaction(tx)

    def _check_free_eligibility(self, project: model.Project, variant: model.Variant,
                                user_id: str, grant_id: str) -> None:
        if variant.kind != "digital_free":
            raise EligibilityRefused("该款式不是免费领取款式")
        if project.claim_rule.get("free_variant_ids") and variant.variant_id not in project.claim_rule["free_variant_ids"]:
            raise EligibilityRefused("该款式当前不在免费领取范围内")
        events = self.store.events_for("entitlement", grant_id)
        if not events or events[0].payload["user_id"] != user_id:
            raise EligibilityRefused("未查询到该用户的有效领取资格")
        scope = events[0].payload["scope"]
        if scope not in (project.project_id, variant.variant_id):
            raise EligibilityRefused("资格范围与领取款式不符")
        if self._grant_used(grant_id):
            raise EligibilityRefused("该领取资格已使用")

    def _check_paid_eligibility(self, project: model.Project, variant: model.Variant,
                                user_id: str, with_physical: bool) -> None:
        if variant.kind != "digital_paid":
            raise EligibilityRefused("付费通道只能购买付费款式")
        if with_physical and not self._physical_available(project):
            raise EligibilityRefused("实体组合套装已约满")

    def _grant_used(self, grant_id: str) -> bool:
        for e in self.store.stream(event_type="ORDER_PLACED"):
            if e.payload.get("grant_id") == grant_id:
                order = self._load_order(e.aggregate_id)
                if order is not None and order.status != model.CANCELLED:
                    return True
        return False

    # ======================================================================
    # 三、下单保留（并发请求只能占一个名额）
    # ======================================================================

    def place_order(self, project_id: str, variant_id: str, user_id: str, *,
                    request_id: str, with_physical: bool = False,
                    grant_id: str | None = None) -> dict[str, Any]:
        """创建订单并占用序号。request_id 为客户端幂等键。"""

        def tx() -> dict[str, Any]:
            # 幂等：同一请求标识重复送达，直接返回首次订单。
            prior = self.store.find_by_causation(request_id)
            first_order = next((e for e in prior if e.event_type == "ORDER_PLACED"), None)
            if first_order is not None:
                p = first_order.payload
                return {"order_id": first_order.aggregate_id, "serial_no": p["serial_no"],
                        "collectible_no": p.get("collectible_no"), "idempotent": True}

            project = self._load_project(project_id)
            variant = project.variants.get(variant_id)
            if variant is None:
                raise NotFound(f"款式不存在：{variant_id}")

            channel = model.FREE if variant.kind == "digital_free" else model.PAID_CHANNEL
            if channel == model.FREE:
                if with_physical:
                    raise EligibilityRefused("实体组合仅随付费订单提供")
                if not grant_id:
                    raise EligibilityRefused("免费领取必须携带领取资格")
                self._check_free_eligibility(project, variant, user_id, grant_id)
            else:
                self._check_paid_eligibility(project, variant, user_id, with_physical)

            # 同一用户在同一款式上只允许存在一个有效订单（免费、付费分别成立）。
            self._check_one_per_user(project_id, variant_id, user_id)

            # 数字序号：取最小可用号，超卖在这里被挡死。
            pool = self._load_digital_pool(variant)
            serial_no = self._first_available(pool)
            if serial_no is None:
                raise SoldOut(f"款式 {variant_id} 已罄")

            order_id = f"ORD-{_uid()}"
            now = self._now()
            expires = (now + self.hold_ttl).isoformat()
            new_events: list = []
            expected: dict[tuple[str, str], int] = {}

            pool_version = self.store.version_of("serial_pool", variant.pool_id)
            new_events.append(envelope(
                "SERIAL_HELD", "serial_pool", variant.pool_id, pool_version + 1,
                f"订单 {order_id} 占用 {variant.name} 序号（{channel}）",
                {"project_id": project_id, "variant_id": variant_id, "serial_no": serial_no,
                 "order_id": order_id, "user_id": user_id, "channel": channel,
                 "expires_at": expires},
                causation_id=request_id, correlation_id=order_id,
            ))
            expected[("serial_pool", variant.pool_id)] = pool_version

            collectible_no = None
            if with_physical:
                phys = self._load_physical_pool(project)
                collectible_no = self._first_available(phys)
                if collectible_no is None:
                    raise SoldOut("实体组合套装已约满")
                phys_version = self.store.version_of("serial_pool", project.physical_pool_id)
                new_events.append(envelope(
                    "PHYSICAL_ALLOCATED", "serial_pool", project.physical_pool_id, phys_version + 1,
                    f"订单 {order_id} 预留实体收藏号 {collectible_no}",
                    {"project_id": project_id, "collectible_no": collectible_no,
                     "order_id": order_id, "user_id": user_id},
                    causation_id=request_id, correlation_id=order_id,
                ))
                expected[("serial_pool", project.physical_pool_id)] = phys_version

            new_events.append(envelope(
                "ORDER_PLACED", "purchase_order", order_id, 1,
                f"用户 {user_id} 下单 {variant.name}，序号 {serial_no}",
                {"project_id": project_id, "variant_id": variant_id, "user_id": user_id,
                 "channel": channel, "price": variant.price, "serial_no": serial_no,
                 "with_physical": with_physical, "collectible_no": collectible_no,
                 "grant_id": grant_id, "hold_expires_at": expires},
                causation_id=request_id, correlation_id=order_id,
            ))
            expected[("purchase_order", order_id)] = 0
            # 免费领取资格核验通过即成交：不进入支付保留窗口，也不会被超时扫描释放。
            if channel == model.FREE:
                new_events.append(envelope(
                    "PAYMENT_SETTLED", "purchase_order", order_id, 2,
                    f"免费领取成交，序号 {serial_no} 正式落定",
                    {"external_payment_id": "FREE-CLAIM", "amount": 0},
                    causation_id=request_id, correlation_id=order_id,
                ))
            self.store.append_many(new_events, expected_versions=expected)
            return {"order_id": order_id, "serial_no": serial_no,
                    "collectible_no": collectible_no, "idempotent": False}

        return self.store.transaction(tx)

    # ======================================================================
    # 四、支付：登记 + 成交；迟到支付不得再次成交
    # ======================================================================

    def payment_callback(self, order_id: str, external_payment_id: str, amount: int, *,
                         succeeded: bool, request_id: str) -> dict[str, Any]:
        """支付渠道回调入口，按 request_id / 渠道流水号双重幂等。"""

        def tx() -> dict[str, Any]:
            prior = self.store.find_by_causation(request_id)
            if prior:
                settled = next((e for e in prior if e.event_type == "PAYMENT_SETTLED"), None)
                if settled:
                    return {"order_id": order_id, "result": "settled", "idempotent": True}
                return {"order_id": order_id, "result": "registered", "idempotent": True}

            order = self._load_order(order_id)
            variant = self._variant_of(order.project_id, order.variant_id)
            if amount != variant.price:
                raise DomainError(f"支付金额与款式价格不符：{amount} != {variant.price}")
            if any(p.external_payment_id == external_payment_id for p in order.payments):
                raise Conflict("同一渠道流水号已登记", code=ALREADY_SETTLED)

            events: list = []
            expected: dict[tuple[str, str], int] = {("purchase_order", order_id): order.version}
            events.append(envelope(
                "PAYMENT_REGISTERED", "purchase_order", order_id, order.version + 1,
                f"收到渠道流水 {external_payment_id} 的支付回执",
                {"external_payment_id": external_payment_id, "amount": amount,
                 "status": "succeeded" if succeeded else "failed"},
                causation_id=request_id, correlation_id=order_id,
            ))
            result = "registered"
            # 关键竞态：订单若已被超时/取消释放，迟到的成功支付不能再次成交。
            if succeeded:
                if order.status == model.CANCELLED:
                    raise Conflict(
                        "订单已因超时或取消释放，迟到支付不予成交，请走原路退款",
                        code=LATE_PAYMENT_REJECTED,
                    )
                if order.status == model.PAID:
                    raise Conflict("订单已支付成交", code=ALREADY_SETTLED)
                events.append(envelope(
                    "PAYMENT_SETTLED", "purchase_order", order_id, order.version + 2,
                    f"订单 {order_id} 支付成交，序号 {order.serial_no} 正式落定",
                    {"external_payment_id": external_payment_id, "amount": amount},
                    causation_id=request_id, correlation_id=order_id,
                ))
                result = "settled"
            self.store.append_many(events, expected_versions=expected)
            return {"order_id": order_id, "result": result, "idempotent": False}

        return self.store.transaction(tx)

    def expire_holds(self) -> list[str]:
        """扫描并释放所有到期未支付的保留，返回被释放的订单号。"""
        now = self._now()
        released: list[str] = []

        def tx() -> None:
            orders = self._all_orders()
            for order in orders:
                if order.status != model.PENDING:
                    continue
                if _parse_ts(order.hold_expires_at) > now:
                    continue
                self._cancel_order_locked(order, reason="hold_timeout")
                released.append(order.order_id)

        self.store.transaction(tx)
        return released

    def cancel_order(self, order_id: str, *, request_id: str, reason: str = "user_cancelled") -> None:
        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            order = self._load_order(order_id)
            if order.status != model.PENDING:
                raise Conflict("只有待支付订单可以取消")
            self._cancel_order_locked(order, reason=reason, causation=request_id)
        self.store.transaction(tx)

    def _cancel_order_locked(self, order: model.Order, *, reason: str,
                             causation: str = "") -> None:
        """超时/取消：订单作废，数字序号与未支付的实体预留号释放回可用池。"""
        project = self._load_project(order.project_id)
        variant = project.variants[order.variant_id]
        events: list = []
        expected: dict[tuple[str, str], int] = {}

        v = self.store.version_of("purchase_order", order.order_id)
        events.append(envelope(
            "ORDER_CANCELLED", "purchase_order", order.order_id, v + 1,
            f"订单 {order.order_id} 取消（{reason}）",
            {"reason": reason, "serial_no": order.serial_no},
            causation_id=causation, correlation_id=order.order_id,
        ))
        expected[("purchase_order", order.order_id)] = v

        pv = self.store.version_of("serial_pool", variant.pool_id)
        events.append(envelope(
            "HOLD_RELEASED", "serial_pool", variant.pool_id, pv + 1,
            f"序号 {order.serial_no} 随订单取消释放",
            {"variant_id": order.variant_id, "serial_no": order.serial_no,
             "order_id": order.order_id, "reason": reason},
            causation_id=causation, correlation_id=order.order_id,
        ))
        expected[("serial_pool", variant.pool_id)] = pv

        if order.with_physical and order.collectible_no:
            phv = self.store.version_of("serial_pool", project.physical_pool_id)
            events.append(envelope(
                "HOLD_RELEASED", "serial_pool", project.physical_pool_id, phv + 1,
                f"实体收藏号 {order.collectible_no} 随未支付订单取消释放",
                {"collectible_no": order.collectible_no, "order_id": order.order_id,
                 "reason": reason},
                causation_id=causation, correlation_id=order.order_id,
            ))
            expected[("serial_pool", project.physical_pool_id)] = phv
        self.store.append_many(events, expected_versions=expected)

    # ======================================================================
    # 五、链上登记：不可删除，只追加纠正
    # ======================================================================

    def request_registration(self, order_id: str, *, request_id: str) -> str:
        registration_id = f"REG-{order_id}"

        def tx() -> str:
            if self.store.find_by_causation(request_id):
                return registration_id
            order = self._load_order(order_id)
            if order.status != model.PAID:
                raise Conflict("只有已支付订单才能发起链上登记")
            if self.store.version_of("registration", registration_id):
                return registration_id
            self.store.append(
                envelope(
                    "REGISTRATION_REQUESTED", "registration", registration_id, 1,
                    f"订单 {order_id} 的序号 {order.serial_no} 请求链上登记",
                    {"order_id": order_id, "serial_no": order.serial_no,
                     "user_id": order.user_id, "variant_id": order.variant_id},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=0,
            )
            return registration_id
        return self.store.transaction(tx)

    def accept_registration(self, order_id: str, token_id: str, *, request_id: str) -> None:
        registration_id = f"REG-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("registration", registration_id)
            reg = model.fold_registration(history)
            assert reg is not None
            if reg.state == model.REG_ACCEPTED:
                return
            if reg.state == model.REG_REVOKED:
                raise Conflict("登记已随退款撤销，迟到的接受回执不得使其复活")
            self.store.append(
                envelope(
                    "REGISTRATION_ACCEPTED", "registration", registration_id,
                    history[-1].version + 1,
                    f"序号 {reg.serial_no} 链上登记成功（token {token_id}）",
                    {"order_id": order_id, "serial_no": reg.serial_no, "token_id": token_id},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    def correct_registration(self, order_id: str, changes: dict[str, Any], *, reason: str,
                             request_id: str, case_id: str = "") -> None:
        """对已接受的登记做追加纠正（如撤销、元数据更正）；原事件保留可溯。"""
        registration_id = f"REG-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("registration", registration_id)
            reg = model.fold_registration(history)
            assert reg is not None
            if reg.state != model.REG_ACCEPTED:
                raise Conflict("只能对已接受的登记追加纠正")
            self.store.append(
                envelope(
                    "REGISTRATION_CORRECTED", "registration", registration_id,
                    history[-1].version + 1,
                    f"序号 {reg.serial_no} 登记追加纠正：{reason}",
                    {"order_id": order_id, "serial_no": reg.serial_no,
                     "changes": changes, "reason": reason, "case_id": case_id},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    # ======================================================================
    # 六、实体仓配：配号 → 打包 → 发货 → 签收 / 退回 / 丢件补发
    # ======================================================================

    def pack_shipment(self, order_id: str) -> str:
        shipment_id = f"SHP-{order_id}"

        def tx() -> str:
            order = self._load_order(order_id)
            if not order.with_physical or not order.collectible_no:
                raise Conflict("该订单不含实体组合")
            if order.status != model.PAID:
                raise Conflict("实体件只能在支付成交后打包")
            version = self.store.version_of("physical_shipment", shipment_id)
            if version:
                return shipment_id
            self.store.append(
                envelope(
                    "PHYSICAL_PACKED", "physical_shipment", shipment_id, 1,
                    f"收藏号 {order.collectible_no} 的实体组合打包",
                    {"order_id": order_id, "collectible_no": order.collectible_no,
                     "attempt": 1, "serial_no": order.serial_no},
                    correlation_id=order_id,
                ),
                expected_version=0,
            )
            return shipment_id
        return self.store.transaction(tx)

    def dispatch_shipment(self, order_id: str, *, carrier: str, waybill_no: str,
                          request_id: str) -> None:
        shipment_id = f"SHP-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("physical_shipment", shipment_id)
            shipment = model.fold_shipment(history)
            assert shipment is not None and shipment.current_attempt is not None
            if shipment.state in ("dispatched", "delivered"):
                return
            if shipment.state != "packed":
                raise Conflict(f"当前物流状态 {shipment.state} 不能发货")
            self.store.append(
                envelope(
                    "PHYSICAL_DISPATCHED", "physical_shipment", shipment_id,
                    history[-1].version + 1,
                    f"收藏号 {shipment.collectible_no} 由 {carrier} 发出，运单 {waybill_no}",
                    {"order_id": order_id, "carrier": carrier, "waybill_no": waybill_no},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    def deliver_shipment(self, order_id: str, *, request_id: str) -> None:
        shipment_id = f"SHP-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("physical_shipment", shipment_id)
            shipment = model.fold_shipment(history)
            assert shipment is not None
            if shipment.state == "delivered":
                return
            if shipment.state != "dispatched":
                raise Conflict(f"当前物流状态 {shipment.state} 不能签收")
            self.store.append(
                envelope(
                    "PHYSICAL_DELIVERED", "physical_shipment", shipment_id,
                    history[-1].version + 1,
                    f"收藏号 {shipment.collectible_no} 已签收",
                    {"order_id": order_id},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    def report_lost(self, order_id: str, *, proof: str, request_id: str) -> None:
        shipment_id = f"SHP-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("physical_shipment", shipment_id)
            shipment = model.fold_shipment(history)
            assert shipment is not None
            if shipment.state not in ("dispatched",):
                raise Conflict("只有在途件可以报丢")
            self.store.append(
                envelope(
                    "PHYSICAL_LOST", "physical_shipment", shipment_id,
                    history[-1].version + 1,
                    f"收藏号 {shipment.collectible_no} 运单丢件，等待补发",
                    {"order_id": order_id, "proof": proof},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    def replace_lost(self, order_id: str, *, request_id: str) -> int:
        """丢件补发：开新的物流批次，收藏序号沿用原号（不重新占号）。"""
        shipment_id = f"SHP-{order_id}"

        def tx() -> int:
            if self.store.find_by_causation(request_id):
                history = self.store.get("physical_shipment", shipment_id)
                return model.fold_shipment(history).current_attempt["attempt"]  # type: ignore[union-attr]
            history = self.store.get("physical_shipment", shipment_id)
            shipment = model.fold_shipment(history)
            assert shipment is not None and shipment.current_attempt is not None
            if shipment.state != "lost":
                raise Conflict("只有丢件状态可以补发")
            attempt = len(shipment.attempts) + 1
            self.store.append(
                envelope(
                    "PHYSICAL_REPLACEMENT_ALLOCATED", "physical_shipment", shipment_id,
                    history[-1].version + 1,
                    f"收藏号 {shipment.collectible_no} 第 {attempt} 批次补发（沿用原序号）",
                    {"order_id": order_id, "attempt": attempt,
                     "collectible_no": shipment.collectible_no, "reason": "lost"},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
            return attempt
        return self.store.transaction(tx)

    def return_shipment(self, order_id: str, *, reason: str, request_id: str) -> None:
        """纸质件退回。若此时数字权益仍有效，即形成「悬空」状态，需客服/仲裁处理；
        若订单已退款撤销，退回件随撤销隔离，编号不再发给任何人。"""
        shipment_id = f"SHP-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("physical_shipment", shipment_id)
            shipment = model.fold_shipment(history)
            assert shipment is not None
            order = self._load_order(order_id)
            if shipment.state not in ("dispatched", "delivered"):
                raise Conflict(f"当前物流状态 {shipment.state} 不能退回")
            events: list = []
            expected = {("physical_shipment", shipment_id): history[-1].version}
            events.append(envelope(
                "PHYSICAL_RETURNED", "physical_shipment", shipment_id,
                history[-1].version + 1,
                f"收藏号 {shipment.collectible_no} 实体件退回（{reason}）",
                {"order_id": order_id, "reason": reason},
                causation_id=request_id, correlation_id=order_id,
            ))
            if order.status == model.REFUNDED:
                project = self._load_project(order.project_id)
                phv = self.store.version_of("serial_pool", project.physical_pool_id)
                events.append(envelope(
                    "PHYSICAL_ISOLATED", "serial_pool", project.physical_pool_id, phv + 1,
                    f"退回的收藏号 {shipment.collectible_no} 随撤销隔离",
                    {"collectible_no": shipment.collectible_no, "order_id": order_id,
                     "reason": f"return_after_refund:{reason}"},
                    causation_id=request_id, correlation_id=order_id,
                ))
                expected[("serial_pool", project.physical_pool_id)] = phv
            self.store.append_many(events, expected_versions=expected)
        self.store.transaction(tx)

    # ======================================================================
    # 七、发票
    # ======================================================================

    def issue_invoice(self, order_id: str, *, request_id: str) -> str:
        invoice_id = f"INV-{order_id}"

        def tx() -> str:
            if self.store.find_by_causation(request_id):
                return invoice_id
            order = self._load_order(order_id)
            if order.status != model.PAID:
                raise Conflict("只有已支付订单可以开票")
            if self.store.version_of("invoice", invoice_id):
                return invoice_id
            self.store.append(
                envelope(
                    "INVOICE_ISSUED", "invoice", invoice_id, 1,
                    f"订单 {order_id} 开具发票 {order.settled_amount} 分",
                    {"order_id": order_id, "amount": order.settled_amount,
                     "user_id": order.user_id},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=0,
            )
            return invoice_id
        return self.store.transaction(tx)

    def void_invoice(self, order_id: str, *, reason: str, request_id: str) -> None:
        invoice_id = f"INV-{order_id}"

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("invoice", invoice_id)
            invoice = model.fold_invoice(history)
            assert invoice is not None
            if invoice.voided:
                return
            self.store.append(
                envelope(
                    "INVOICE_VOIDED", "invoice", invoice_id, history[-1].version + 1,
                    f"发票 {invoice_id} 冲红（{reason}）",
                    {"order_id": order_id, "reason": reason},
                    causation_id=request_id, correlation_id=order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    # ======================================================================
    # 八、退款与撤销
    # ======================================================================

    def refund_order(self, order_id: str, *, external_payment_id: str, reason: str,
                     request_id: str) -> None:
        """退款撤销：资金退回、发票冲红、数字登记追加撤销；未发实体隔离。
        已发出的实体件不在此步处理，待退回或仲裁裁决。"""

        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            order = self._load_order(order_id)
            if order.status != model.PAID:
                raise Conflict("只有已支付订单可以退款")
            events: list = []
            expected: dict[tuple[str, str], int] = {("purchase_order", order_id): order.version}
            events.append(envelope(
                "PAYMENT_REFUNDED", "purchase_order", order_id, order.version + 1,
                f"订单 {order_id} 退款撤销（{reason}）",
                {"external_payment_id": external_payment_id,
                 "amount": order.settled_amount, "reason": reason},
                causation_id=request_id, correlation_id=order_id,
            ))

            # 数字编号撤销登记并隔离：该编号历史可溯，但不会再分配给第二位购买者。
            reg_history = self.store.events_for("registration", f"REG-{order_id}")
            reg = model.fold_registration(reg_history) if reg_history else None
            if reg is not None and reg.state != model.REG_REVOKED:
                events.append(envelope(
                    "REGISTRATION_CORRECTED", "registration", f"REG-{order_id}",
                    reg_history[-1].version + 1,
                    f"序号 {order.serial_no} 因退款撤销登记",
                    {"order_id": order_id, "serial_no": order.serial_no,
                     "changes": {"state": model.REG_REVOKED}, "reason": f"refund:{reason}"},
                    causation_id=request_id, correlation_id=order_id,
                ))
                expected[("registration", f"REG-{order_id}")] = reg_history[-1].version

            variant = self._variant_of(order.project_id, order.variant_id)
            pv = self.store.version_of("serial_pool", variant.pool_id)
            events.append(envelope(
                "SERIAL_ISOLATED", "serial_pool", variant.pool_id, pv + 1,
                f"已售序号 {order.serial_no} 因退款隔离，不再分配",
                {"variant_id": order.variant_id, "serial_no": order.serial_no,
                 "order_id": order_id, "reason": f"refund:{reason}"},
                causation_id=request_id, correlation_id=order_id,
            ))
            expected[("serial_pool", variant.pool_id)] = pv

            # 未发实体（未打包或仅打包未发出）随撤销隔离；在途/已签收件等待退回或仲裁。
            shipment_state = self._shipment_state(order_id)
            unshipped_states = (None, "allocated", "packed")
            if order.with_physical and order.collectible_no and shipment_state in unshipped_states:
                project = self._load_project(order.project_id)
                phv = self.store.version_of("serial_pool", project.physical_pool_id)
                events.append(envelope(
                    "PHYSICAL_ISOLATED", "serial_pool", project.physical_pool_id, phv + 1,
                    f"未发出的收藏号 {order.collectible_no} 随撤销隔离",
                    {"collectible_no": order.collectible_no, "order_id": order_id,
                     "reason": f"refund_before_ship:{reason}"},
                    causation_id=request_id, correlation_id=order_id,
                ))
                expected[("serial_pool", project.physical_pool_id)] = phv

            inv_history = self.store.events_for("invoice", f"INV-{order_id}")
            invoice = model.fold_invoice(inv_history) if inv_history else None
            if invoice is not None and not invoice.voided:
                events.append(envelope(
                    "INVOICE_VOIDED", "invoice", f"INV-{order_id}", inv_history[-1].version + 1,
                    f"发票随退款冲红（{reason}）",
                    {"order_id": order_id, "reason": f"refund:{reason}"},
                    causation_id=request_id, correlation_id=order_id,
                ))
                expected[("invoice", f"INV-{order_id}")] = inv_history[-1].version

            self.store.append_many(events, expected_versions=expected)
        self.store.transaction(tx)

    # ======================================================================
    # 九、仲裁
    # ======================================================================

    def open_arbitration(self, order_id: str, *, reason: str) -> str:
        case_id = f"CASE-{_uid()}"

        def tx() -> str:
            self._load_order(order_id)
            self.store.append(
                envelope(
                    "ARBITRATION_OPENED", "fulfillment_case", case_id, 1,
                    f"订单 {order_id} 进入仲裁：{reason}",
                    {"order_id": order_id, "reason": reason},
                    correlation_id=order_id,
                ),
                expected_version=0,
            )
            return case_id
        return self.store.transaction(tx)

    def apply_remedy(self, case_id: str, action: str, *, note: str = "",
                     request_id: str = "") -> None:
        """登记仲裁救济动作（resend / revoke / release_isolated 等），追加留痕。"""

        def tx() -> None:
            history = self.store.get("fulfillment_case", case_id)
            case = model.fold_case(history)
            assert case is not None
            if not case.open:
                raise Conflict("仲裁案件已结案，救济请另开案件")
            self.store.append(
                envelope(
                    "ORDER_REMEDIED", "fulfillment_case", case_id, history[-1].version + 1,
                    f"案件 {case_id} 救济动作 {action}",
                    {"order_id": case.order_id, "action": action, "note": note},
                    causation_id=request_id, correlation_id=case.order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    def resolve_arbitration(self, case_id: str, *, ruling: str, request_id: str) -> None:
        def tx() -> None:
            if self.store.find_by_causation(request_id):
                return
            history = self.store.get("fulfillment_case", case_id)
            case = model.fold_case(history)
            assert case is not None
            if not case.open:
                return
            self.store.append(
                envelope(
                    "ARBITRATION_RESOLVED", "fulfillment_case", case_id,
                    history[-1].version + 1,
                    f"案件 {case_id} 结案：{ruling}",
                    {"order_id": case.order_id, "ruling": ruling},
                    causation_id=request_id, correlation_id=case.order_id,
                ),
                expected_version=history[-1].version,
            )
        self.store.transaction(tx)

    # ======================================================================
    # 折叠辅助
    # ======================================================================

    def _load_project(self, project_id: str) -> model.Project:
        events = self.store.get("project", project_id)
        project = model.fold_project(events)
        assert project is not None
        for ve in self.store.stream(event_type="VARIANT_CONFIGURED"):
            if ve.payload["project_id"] == project_id and ve.aggregate_id not in project.variants:
                variant = model.fold_variant([ve])
                assert variant is not None
                project.variants[variant.variant_id] = variant
        return project

    def _variant_of(self, project_id: str, variant_id: str) -> model.Variant:
        history = self.store.get("edition", variant_id)
        variant = model.fold_variant(history)
        if variant is None or variant.kind not in ("digital_paid", "digital_free"):
            raise NotFound(f"款式不存在：{variant_id}")
        return variant

    def _load_order(self, order_id: str) -> model.Order:
        history = self.store.get("purchase_order", order_id)
        order = model.fold_order(history)
        assert order is not None
        order.version = history[-1].version
        return order

    def _all_orders(self) -> list[model.Order]:
        orders = []
        seen: set[str] = set()
        for e in self.store.stream(event_type="ORDER_PLACED"):
            if e.aggregate_id in seen:
                continue
            seen.add(e.aggregate_id)
            order = model.fold_order(self.store.events_for("purchase_order", e.aggregate_id))
            if order is not None:
                orders.append(order)
        return orders

    def _load_digital_pool(self, variant: model.Variant) -> model.SerialLedger:
        return model.fold_serial_pool(
            self.store.events_for("serial_pool", variant.pool_id), variant.pool_id, variant.cap
        )

    def _load_physical_pool(self, project: model.Project) -> model.SerialLedger:
        return model.fold_physical_pool(
            self.store.events_for("serial_pool", project.physical_pool_id),
            project.physical_pool_id, project.physical_cap,
        )

    def _physical_available(self, project: model.Project) -> bool:
        return self._first_available(self._load_physical_pool(project)) is not None

    @staticmethod
    def _first_available(pool: model.SerialLedger) -> str | None:
        for no in range(1, pool.cap + 1):
            formatted = f"{no:04d}"
            if pool.status_of(formatted) == model.AVAILABLE:
                return formatted
        return None

    def _check_one_per_user(self, project_id: str, variant_id: str, user_id: str) -> None:
        for e in self.store.stream(event_type="ORDER_PLACED"):
            p = e.payload
            if p["project_id"] == project_id and p["variant_id"] == variant_id and p["user_id"] == user_id:
                order = self._load_order(e.aggregate_id)
                if order.status != model.CANCELLED:
                    raise Conflict("同一用户在该款式已持有有效订单，并发请求只能占一个名额")

    def _shipment_state(self, order_id: str) -> str | None:
        history = self.store.events_for("physical_shipment", f"SHP-{order_id}")
        if not history:
            return None
        shipment = model.fold_shipment(history)
        return shipment.state if shipment else None
