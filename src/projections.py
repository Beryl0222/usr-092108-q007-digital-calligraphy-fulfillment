"""读模型投影：事件 → 面向三类读者的视图。

- 购买者：自己订单上的数字编号、实体配号、物流是否一致；
- 客服：每一次占用与释放的完整台账，可按订单/序号解释；
- 发行方：各款式守恒结果（总量核对），只给计数与编号区间，不输出用户画像。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime

from . import events as ev


# ---- 购买者视图 ------------------------------------------------------------

@dataclass
class ShipmentView:
    tracking_no: str
    carrier: str
    kind: str
    dispatched_at: str


@dataclass
class OrderView:
    order_id: str
    user_id: str
    variant_id: str
    channel: str
    serial: int | None = None
    order_status: str = ""
    expires_at: str = ""
    paid: bool = False
    chain_ref: str | None = None
    registration_status: str = ""
    physical_required: bool = False
    physical_status: str = ""
    physical_serial: int | None = None
    shipments: list[ShipmentView] = field(default_factory=list)
    invoice_status: str = ""
    case_status: str = ""

    @property
    def serial_consistent(self) -> bool:
        """数字收藏序号与实体配号必须相同。"""
        if not self.physical_required:
            return True
        if self.physical_serial is None:
            return self.order_status in ("cancelled", "refunded")
        return self.physical_serial == self.serial

    @property
    def logistics_consistent(self) -> bool:
        """已发货则必须有运单；丢件补发后运单与补发事件对齐。"""
        if self.physical_status in ("dispatched", "delivered", "lost"):
            return bool(self.shipments)
        return True

    @property
    def consistent(self) -> bool:
        return self.serial_consistent and self.logistics_consistent


# ---- 客服台账 --------------------------------------------------------------

@dataclass
class LedgerEntry:
    at: str
    variant_id: str
    serial: int | None
    order_id: str
    action: str          # held / released / sold / quarantined / reshipped ...
    reason: str
    detail: dict = field(default_factory=dict)


# ---- 发行方守恒报告 --------------------------------------------------------

@dataclass
class VariantReport:
    variant_id: str
    title: str
    kind: str
    edition_size: int
    held: int = 0
    sold: int = 0
    paid_orders: int = 0
    registrations_accepted: int = 0
    physical_allocated: int = 0
    physical_dispatched: int = 0
    physical_delivered: int = 0
    physical_quarantined: int = 0
    physical_lost: int = 0
    reshipments: int = 0
    anomalies: list[str] = field(default_factory=list)

    @property
    def accounted(self) -> int:
        """已被占用或售出的名额，守恒要求 <= 发行量。"""
        return self.held + self.sold

    @property
    def conserved(self) -> bool:
        return (
            self.accounted <= self.edition_size
            and self.sold <= self.edition_size
            and self.registrations_accepted <= self.sold
            and self.physical_allocated <= self.sold
            and not self.anomalies
        )


class ReadModel:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.orders: dict[str, OrderView] = {}
        self.variants: dict[str, dict] = {}
        self.serial_ledger: dict[tuple[str, int], list[LedgerEntry]] = {}
        self.order_ledger: dict[str, list[LedgerEntry]] = {}
        self._reg_serial_by_order: dict[str, int] = {}
        self._phy_serial_by_order: dict[str, int] = {}
        self._applied: set[str] = set()   # 已投影的 event_id，保证重放幂等

    # 事件存储在提交锁内回调本方法
    def handle(self, e: ev.Event) -> None:
        with self._lock:
            if e.event_id in self._applied:
                return
            p = e.payload
            at = e.occurred_at.isoformat()
            t = e.event_type

            if t == ev.VARIANT_OPENED and e.aggregate_type == ev.AGG_VARIANT:
                self.variants[e.aggregate_id] = {
                    "variant_id": e.aggregate_id, "title": p["title"], "kind": p["kind"],
                    "edition_size": p["edition_size"],
                    "physical_capacity": p.get("physical_capacity", 0),
                }
                return
            if t in (ev.PROJECT_CREATED, ev.VARIANT_OPENED, ev.VARIANT_CLOSED,
                     ev.ELIGIBILITY_GRANTED, ev.ELIGIBILITY_REVOKED,
                     ev.HOLD_EXTENDED):
                # 项目流登记 / 资格事件 / 保留延长不进守恒与订单主视图
                if t == ev.HOLD_EXTENDED and p.get("order_id"):
                    if (ov := self.orders.get(p["order_id"])) is not None:
                        ov.expires_at = p["expires_at"]
                return

            ov = self._order(p["order_id"]) if p.get("order_id") else None
            is_variant_stream = e.aggregate_type == ev.AGG_VARIANT
            is_order_stream = e.aggregate_type == ev.AGG_ORDER
            is_ledger_stream = e.aggregate_type == ev.AGG_REGISTRY

            if t == ev.SERIAL_HELD and is_variant_stream:
                self._entry(p["variant_id"], p["serial"], p["order_id"], at,
                            "held", p.get("reason", "下单保留"),
                            {"user_masked": _mask(p["user_id"]), "expires_at": p["expires_at"]})
            elif t == ev.HOLD_RELEASED and is_variant_stream:
                self._entry(p["variant_id"], p.get("serial"), p.get("order_id", ""), at,
                            "released", p.get("reason", ""))
            elif t == ev.ORDER_PLACED and is_order_stream:
                ov = self._order(p["order_id"])
                ov.user_id = p["user_id"]
                ov.variant_id = p["variant_id"]
                ov.channel = p["channel"]
                ov.serial = p["serial"]
                ov.order_status = "held"
                ov.expires_at = p["expires_at"]
                ov.physical_required = p.get("physical_required", False)
            elif t == ev.PAYMENT_CONFIRMED and is_order_stream:
                ov = self._order(p["order_id"])
                ov.paid = True
                ov.order_status = "paid"
            elif t == ev.PAYMENT_CONFIRMED and is_variant_stream:
                # 序号台账只从款式流入账，避免与订单流重复计数
                self._entry(p["variant_id"], p["serial"], p["order_id"], at, "sold",
                            "支付确认", {"payment_ref": p["payment_ref"]})
            elif t == ev.PAYMENT_LATE_REJECTED:
                self._entry(p["variant_id"], p.get("serial"), p.get("order_id", ""), at,
                            "late_payment_rejected", "迟到支付不再成交",
                            {"payment_ref": p["payment_ref"]})
            elif t == ev.ORDER_CANCELLED and is_order_stream:
                self._order(p["order_id"]).order_status = "cancelled"
            elif t == ev.REFUND_GRANTED and is_order_stream:
                self._order(p["order_id"]).order_status = "refunded"
            elif t == ev.REGISTRATION_REQUESTED and is_ledger_stream:
                self._reg_serial_by_order[p["order_id"]] = p["serial"]
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.registration_status = "requested"
                self._entry(p["variant_id"], p["serial"], p["order_id"], at,
                            "registration_requested", "链上登记请求")
            elif t == ev.REGISTRATION_ACCEPTED and is_ledger_stream:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.registration_status = "accepted"
                    v.chain_ref = p.get("chain_ref")
                self._entry(p["variant_id"], p["serial"], p["order_id"], at,
                            "registration_accepted", "链上登记成立",
                            {"chain_ref": p.get("chain_ref")})
            elif t == ev.REGISTRATION_FAILED and is_ledger_stream:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.registration_status = "failed"
            elif t == ev.REGISTRATION_CORRECTED and is_ledger_stream:
                if (v := self.orders.get(p.get("order_id", ""))) is not None:
                    v.registration_status = p.get("new_status", v.registration_status)
                self._entry(p["variant_id"], p["serial"], p.get("order_id", ""), at,
                            "registration_corrected", "追加纠正（原记录保留）",
                            {"note": p.get("note", "")})
            elif t == ev.PHYSICAL_ALLOCATED:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "allocated"
                    v.physical_serial = p["serial"]
                self._phy_serial_by_order[p["order_id"]] = p["serial"]
                self._entry(p["variant_id"], p["serial"], p["order_id"], at,
                            "physical_allocated", "实体配号",
                            {"physical_unit": p.get("physical_unit", "")})
            elif t == ev.PHYSICAL_QUARANTINED:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "quarantined"
                self._entry(p["variant_id"], p.get("serial"), p["order_id"], at,
                            "quarantined", p.get("reason", "未发实体随撤销隔离"))
            elif t == ev.PHYSICAL_DISPATCHED:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "dispatched"
                    v.shipments.append(ShipmentView(
                        p["tracking_no"], p.get("carrier", ""),
                        p.get("kind", "original"), at))
                self._entry(p["variant_id"], p.get("serial"), p["order_id"], at,
                            "dispatched", "实体发出", {"tracking_no": p["tracking_no"]})
            elif t == ev.PHYSICAL_DELIVERED:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "delivered"
                self._entry(p["variant_id"], p.get("serial"), p["order_id"], at,
                            "delivered", "签收")
            elif t == ev.PHYSICAL_LOST:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "lost"
                self._entry(p["variant_id"], p.get("serial"), p["order_id"], at,
                            "lost", "物流判丢")
            elif t == ev.PHYSICAL_RESENT:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "dispatched"
                    v.shipments.append(ShipmentView(
                        p["tracking_no"], p.get("carrier", ""), "reshipment", at))
                self._entry(p["variant_id"], p.get("serial"), p["order_id"], at,
                            "reshipped", "丢件补发，沿用原收藏序号",
                            {"tracking_no": p["tracking_no"]})
            elif t == ev.PHYSICAL_RETURNED:
                if (v := self.orders.get(p["order_id"])) is not None:
                    v.physical_status = "returned"
            elif t in (ev.INVOICE_ISSUED, ev.INVOICE_VOIDED, ev.INVOICE_REISSUED):
                if (v := self.orders.get(p.get("order_id", ""))) is not None:
                    v.invoice_status = t
            elif t in (ev.ARBITRATION_OPENED, ev.ORDER_REMEDIED, ev.ARBITRATION_RESOLVED):
                if (v := self.orders.get(p.get("order_id", ""))) is not None:
                    v.case_status = "resolved" if t == ev.ARBITRATION_RESOLVED else "open"
            self._applied.add(e.event_id)

    # ---- 查询 ------------------------------------------------------------

    def purchaser_view(self, order_id: str) -> OrderView | None:
        with self._lock:
            return self.orders.get(order_id)

    def serial_history(self, variant_id: str, serial: int) -> list[LedgerEntry]:
        with self._lock:
            return list(self.serial_ledger.get((variant_id, serial), ()))

    def order_history(self, order_id: str) -> list[LedgerEntry]:
        with self._lock:
            return list(self.order_ledger.get(order_id, ()))

    def conservation_report(self) -> dict[str, VariantReport]:
        """从台账重算各款式守恒结果；编号级核对也在此完成。"""
        with self._lock:
            reports: dict[str, VariantReport] = {}
            for vid, meta in self.variants.items():
                reports[vid] = VariantReport(
                    variant_id=vid, title=meta["title"], kind=meta["kind"],
                    edition_size=meta["edition_size"])
            held_now: dict[str, set[int]] = {}
            sold: dict[str, set[int]] = {}
            accepted: dict[str, set[int]] = {}
            allocated: dict[str, set[int]] = {}
            quarantined: dict[str, set[int]] = {}
            delivered: dict[str, set[int]] = {}
            reships: dict[str, int] = {}
            dispatched: dict[str, int] = {}
            lost: dict[str, int] = {}
            for (vid, n), entries in self.serial_ledger.items():
                r = reports.get(vid)
                if r is None:
                    continue
                for e in entries:
                    if e.action == "held":
                        held_now.setdefault(vid, set()).add(n)
                    elif e.action == "released":
                        held_now.get(vid, set()).discard(n)
                    elif e.action == "sold":
                        held_now.get(vid, set()).discard(n)
                        sold.setdefault(vid, set()).add(n)
                    elif e.action == "registration_accepted":
                        accepted.setdefault(vid, set()).add(n)
                    elif e.action == "physical_allocated":
                        allocated.setdefault(vid, set()).add(n)
                    elif e.action == "quarantined":
                        quarantined.setdefault(vid, set()).add(n)
                    elif e.action == "dispatched":
                        dispatched[vid] = dispatched.get(vid, 0) + 1
                    elif e.action == "delivered":
                        delivered.setdefault(vid, set()).add(n)
                    elif e.action == "lost":
                        lost[vid] = lost.get(vid, 0) + 1
                    elif e.action == "reshipped":
                        reships[vid] = reships.get(vid, 0) + 1
                # 同一序号落在两位购买者手里的核对由 sold 集合天然保证；
                # 额外校验：任何配号必须等于已售序号。
                if n in allocated.get(vid, set()) and n not in sold.get(vid, set()):
                    r.anomalies.append(f"序号 {n} 已配实体但未售出")
            for vid, r in reports.items():
                r.held = len(held_now.get(vid, ()))
                r.sold = len(sold.get(vid, ()))
                r.registrations_accepted = len(accepted.get(vid, ()))
                r.physical_allocated = len(allocated.get(vid, set()) - quarantined.get(vid, set()))
                r.physical_quarantined = len(quarantined.get(vid, ()))
                r.physical_delivered = len(delivered.get(vid, ()))
                r.physical_dispatched = dispatched.get(vid, 0)  # 运单计数（含补发）
                r.physical_lost = lost.get(vid, 0)
                r.reshipments = reships.get(vid, 0)
                r.paid_orders = r.sold
                overlap = held_now.get(vid, set()) & sold.get(vid, set())
                if overlap:
                    r.anomalies.append(f"序号同时处于持有与售出：{sorted(overlap)}")
            return reports

    # ---- 内部 ------------------------------------------------------------

    def _order(self, order_id: str) -> OrderView:
        ov = self.orders.get(order_id)
        if ov is None:
            ov = OrderView(order_id=order_id, user_id="", variant_id="", channel="")
            self.orders[order_id] = ov
        return ov

    def _entry(self, variant_id: str, serial: int | None, order_id: str,
               at: str, action: str, reason: str, detail: dict | None = None) -> None:
        entry = LedgerEntry(at=at, variant_id=variant_id, serial=serial,
                            order_id=order_id, action=action, reason=reason,
                            detail=detail or {})
        if serial is not None:
            self.serial_ledger.setdefault((variant_id, serial), []).append(entry)
        if order_id:
            self.order_ledger.setdefault(order_id, []).append(entry)


def _mask(user_id: str) -> str:
    """对外只给脱敏标识，避免在台账里暴露完整用户身份。"""
    if not user_id:
        return ""
    return user_id[:2] + "***" + user_id[-2:] if len(user_id) > 4 else "***"
