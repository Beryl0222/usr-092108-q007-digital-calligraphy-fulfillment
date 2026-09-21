"""领域状态与事件重放（纯函数式投影到聚合内存状态）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from . import events as ev


# ---- 枚举型状态常量 --------------------------------------------------------

KIND_PAID = "paid"        # 四款付费数字作品
KIND_FREE = "free"        # 免费限领版本
KIND_BUNDLE = "bundle"    # 一千套实体组合（含数字编号）

VARIANT_DRAFT = "draft"
VARIANT_OPEN = "open"
VARIANT_CLOSED = "closed"

ORDER_HELD = "held"            # 已占号，待支付
ORDER_PAID = "paid"
ORDER_CANCELLED = "cancelled"
ORDER_REFUNDED = "refunded"

CANCEL_TIMEOUT = "hold_timeout"      # 超时释放
CANCEL_USER = "user_cancelled"       # 用户取消
CANCEL_ADMIN = "admin_revoked"       # 管理撤销

PHY_ALLOCATED = "allocated"
PHY_QUARANTINED = "quarantined"
PHY_DISPATCHED = "dispatched"
PHY_DELIVERED = "delivered"
PHY_LOST = "lost"
PHY_RETURNED = "returned"

REG_REQUESTED = "requested"
REG_ACCEPTED = "accepted"
REG_FAILED = "failed"
REG_CORRECTED = "corrected"

INVOICE_REQUESTED = "requested"
INVOICE_ISSUED = "issued"
INVOICE_VOIDED = "voided"
INVOICE_CREDIT = "credit"           # 退款后重开的红票/贷记凭证

ARBITRATION_OPEN = "open"
ARBITRATION_RESOLVED = "resolved"


@dataclass
class ProjectState:
    project_id: str
    title: str = ""
    variant_ids: list[str] = field(default_factory=list)
    created: bool = False


@dataclass
class VariantState:
    variant_id: str
    project_id: str = ""
    title: str = ""
    kind: str = KIND_PAID
    edition_size: int = 0
    physical_capacity: int = 0
    requires_eligibility: bool = False
    status: str = VARIANT_DRAFT
    # serial(1..size) -> 状态
    serial_status: dict[int, str] = field(default_factory=dict)   # held / sold
    holder_order: dict[int, str] = field(default_factory=dict)
    holder_user: dict[int, str] = field(default_factory=dict)
    holder_expires: dict[int, datetime] = field(default_factory=dict)
    owner_user: dict[int, str] = field(default_factory=dict)      # sold 后永不移除
    order_serial: dict[str, int] = field(default_factory=dict)

    def free_serials(self) -> list[int]:
        return [n for n in range(1, self.edition_size + 1) if n not in self.serial_status]

    def serial_of_user(self, user_id: str) -> int | None:
        """用户当前在本款式占有的序号（持有或已购），用于一人一名额。"""
        for n, uid in self.holder_user.items():
            if uid == user_id:
                return n
        for n, uid in self.owner_user.items():
            if uid == user_id:
                return n
        return None


@dataclass
class EligibilityState:
    key: str
    variant_id: str = ""
    user_id: str = ""
    active: bool = False
    grant_kind: str = ""          # free_claim / purchase
    verified: bool = False
    channel: str = ""


@dataclass
class OrderState:
    order_id: str
    project_id: str = ""
    variant_id: str = ""
    user_id: str = ""
    channel: str = KIND_PAID
    price: int = 0                # 分为单位
    serial: int | None = None
    status: str = ""
    created_at: datetime | None = None
    expires_at: datetime | None = None
    paid_at: datetime | None = None
    payment_refs: set[str] = field(default_factory=set)
    rejected_payments: set[str] = field(default_factory=set)
    physical_required: bool = False
    refund_reason: str = ""
    registrations_requested: set[str] = field(default_factory=set)

    @property
    def is_held(self) -> bool:
        return self.status == ORDER_HELD


@dataclass
class RegistrationRecord:
    serial: int
    order_id: str
    status: str = REG_REQUESTED
    chain_ref: str | None = None
    note: str = ""


@dataclass
class LedgerState:
    variant_id: str
    records: dict[int, RegistrationRecord] = field(default_factory=dict)
    history: list[dict] = field(default_factory=list)   # 追加式全量轨迹


@dataclass
class Shipment:
    tracking_no: str
    carrier: str
    kind: str = "original"          # original / reshipment
    dispatched_at: datetime | None = None


@dataclass
class FulfillmentState:
    fulfillment_id: str
    order_id: str = ""
    variant_id: str = ""
    serial: int | None = None       # 实体配号 = 数字收藏序号
    status: str = ""
    physical_unit: str = ""
    shipments: list[Shipment] = field(default_factory=list)
    quarantine_reason: str = ""


@dataclass
class InvoiceState:
    invoice_id: str
    order_id: str = ""
    status: str = ""
    title: str = ""
    amount: int = 0
    tax_no: str = ""
    related_invoice_id: str | None = None


@dataclass
class ArbitrationState:
    case_id: str
    order_id: str = ""
    status: str = ""
    reason: str = ""
    resolution: str = ""
    note: str = ""


# ---- 单流重放 --------------------------------------------------------------

def _require(state: object | None, cls: type):
    if not isinstance(state, cls):
        return cls()  # type: ignore[call-arg]
    return state


def replay(aggregate_type: str, aggregate_id: str, events: list[ev.Event]) -> object | None:
    if not events:
        return None
    if aggregate_type == ev.AGG_PROJECT:
        return _replay_project(aggregate_id, events)
    if aggregate_type == ev.AGG_VARIANT:
        return _replay_variant(aggregate_id, events)
    if aggregate_type == ev.AGG_ELIGIBILITY:
        return replay_eligibility(aggregate_id, events)
    if aggregate_type == ev.AGG_ORDER:
        return _replay_order(aggregate_id, events)
    if aggregate_type == ev.AGG_REGISTRY:
        return _replay_ledger(aggregate_id, events)
    if aggregate_type == ev.AGG_FULFILLMENT:
        return _replay_fulfillment(aggregate_id, events)
    if aggregate_type == ev.AGG_INVOICE:
        return _replay_invoice(aggregate_id, events)
    if aggregate_type == ev.AGG_CASE:
        return _replay_arbitration(aggregate_id, events)
    return None


def _replay_project(pid: str, events: list[ev.Event]) -> ProjectState:
    s = ProjectState(project_id=pid)
    for e in events:
        if e.event_type == ev.PROJECT_CREATED:
            s.created = True
            s.title = e.payload.get("title", "")
        elif e.event_type == ev.VARIANT_OPENED:
            if e.payload.get("variant_id") not in s.variant_ids:
                s.variant_ids.append(e.payload["variant_id"])
        elif e.event_type == ev.VARIANT_CLOSED:
            if e.payload.get("variant_id") in s.variant_ids:
                pass
    return s


def _replay_variant(vid: str, events: list[ev.Event]) -> VariantState:
    s = VariantState(variant_id=vid)
    for e in events:
        p = e.payload
        if e.event_type == ev.VARIANT_OPENED:
            s.project_id = p["project_id"]
            s.title = p["title"]
            s.kind = p["kind"]
            s.edition_size = p["edition_size"]
            s.physical_capacity = p.get("physical_capacity", 0)
            s.requires_eligibility = p.get("requires_eligibility", False)
            s.status = VARIANT_OPEN
        elif e.event_type == ev.VARIANT_CLOSED:
            s.status = VARIANT_CLOSED
        elif e.event_type == ev.SERIAL_HELD:
            n = p["serial"]
            s.serial_status[n] = "held"
            s.holder_order[n] = p["order_id"]
            s.holder_user[n] = p["user_id"]
            s.holder_expires[n] = datetime.fromisoformat(p["expires_at"])
            s.order_serial[p["order_id"]] = n
        elif e.event_type == ev.HOLD_RELEASED:
            n = p.get("serial") or s.order_serial.get(p.get("order_id", ""))
            if n is not None:
                s.serial_status.pop(n, None)
                s.holder_order.pop(n, None)
                s.holder_user.pop(n, None)
                s.holder_expires.pop(n, None)
                oid = next((o for o, x in s.order_serial.items() if x == n), None)
                if oid:
                    s.order_serial.pop(oid, None)
        elif e.event_type == ev.PAYMENT_CONFIRMED:
            n = p["serial"]
            s.serial_status[n] = "sold"
            s.owner_user[n] = p["user_id"]
            s.holder_expires.pop(n, None)
            # holder 字段保留到交割语义结束，订单号仍可追溯
    return s


def _replay_order(oid: str, events: list[ev.Event]) -> OrderState:
    s = OrderState(order_id=oid)
    for e in events:
        p = e.payload
        if e.event_type == ev.ORDER_PLACED:
            s.project_id = p["project_id"]
            s.variant_id = p["variant_id"]
            s.user_id = p["user_id"]
            s.channel = p["channel"]
            s.price = p.get("price", 0)
            s.serial = p["serial"]
            s.status = ORDER_HELD
            s.created_at = e.occurred_at
            s.expires_at = datetime.fromisoformat(p["expires_at"])
            s.physical_required = p.get("physical_required", False)
        elif e.event_type == ev.HOLD_EXTENDED:
            s.expires_at = datetime.fromisoformat(p["expires_at"])
        elif e.event_type == ev.PAYMENT_CONFIRMED:
            s.status = ORDER_PAID
            s.paid_at = e.occurred_at
            s.payment_refs.add(p["payment_ref"])
        elif e.event_type == ev.PAYMENT_LATE_REJECTED:
            s.rejected_payments.add(p["payment_ref"])
        elif e.event_type == ev.ORDER_CANCELLED:
            s.status = ORDER_CANCELLED
        elif e.event_type == ev.REFUND_GRANTED:
            s.status = ORDER_REFUNDED
            s.refund_reason = p.get("reason", "")
        elif e.event_type == ev.REGISTRATION_REQUESTED:
            s.registrations_requested.add(p.get("request_id", ""))
    return s


def _replay_ledger(vid: str, events: list[ev.Event]) -> LedgerState:
    s = LedgerState(variant_id=vid)
    for e in events:
        p = e.payload
        n = p.get("serial")
        s.history.append({"event_type": e.event_type, "payload": dict(p),
                          "occurred_at": e.occurred_at.isoformat()})
        if e.event_type == ev.REGISTRATION_REQUESTED:
            rec = s.records.get(n) or RegistrationRecord(serial=n, order_id=p["order_id"])
            rec.status = REG_REQUESTED
            s.records[n] = rec
        elif e.event_type == ev.REGISTRATION_ACCEPTED:
            rec = s.records.get(n) or RegistrationRecord(serial=n, order_id=p["order_id"])
            rec.status = REG_ACCEPTED
            rec.chain_ref = p.get("chain_ref")
            s.records[n] = rec
        elif e.event_type == ev.REGISTRATION_FAILED:
            rec = s.records.get(n) or RegistrationRecord(serial=n, order_id=p["order_id"])
            rec.status = REG_FAILED
            rec.note = p.get("reason", "")
            s.records[n] = rec
        elif e.event_type == ev.REGISTRATION_CORRECTED:
            rec = s.records.get(n) or RegistrationRecord(serial=n, order_id=p.get("order_id", ""))
            # 追加纠正不覆盖链上引用，只更新当前有效状态与备注
            if p.get("new_status"):
                rec.status = p["new_status"]
            rec.note = p.get("note", rec.note)
            s.records[n] = rec
    return s


def _replay_fulfillment(fid: str, events: list[ev.Event]) -> FulfillmentState:
    s = FulfillmentState(fulfillment_id=fid)
    for e in events:
        p = e.payload
        if e.event_type == ev.PHYSICAL_ALLOCATED:
            s.order_id = p["order_id"]
            s.variant_id = p["variant_id"]
            s.serial = p["serial"]
            s.physical_unit = p.get("physical_unit", "")
            s.status = PHY_ALLOCATED
        elif e.event_type == ev.PHYSICAL_QUARANTINED:
            s.status = PHY_QUARANTINED
            s.quarantine_reason = p.get("reason", "")
        elif e.event_type == ev.PHYSICAL_DISPATCHED:
            s.status = PHY_DISPATCHED
            s.shipments.append(Shipment(
                tracking_no=p["tracking_no"], carrier=p.get("carrier", ""),
                kind=p.get("kind", "original"), dispatched_at=e.occurred_at))
        elif e.event_type == ev.PHYSICAL_DELIVERED:
            s.status = PHY_DELIVERED
        elif e.event_type == ev.PHYSICAL_LOST:
            s.status = PHY_LOST
        elif e.event_type == ev.PHYSICAL_RESENT:
            s.shipments.append(Shipment(
                tracking_no=p["tracking_no"], carrier=p.get("carrier", ""),
                kind="reshipment", dispatched_at=e.occurred_at))
            s.status = PHY_DISPATCHED
        elif e.event_type == ev.PHYSICAL_RETURNED:
            s.status = PHY_RETURNED
    return s


def _replay_invoice(iid: str, events: list[ev.Event]) -> InvoiceState:
    s = InvoiceState(invoice_id=iid)
    for e in events:
        p = e.payload
        if e.event_type == ev.INVOICE_REQUESTED:
            s.order_id = p["order_id"]
            s.title = p.get("title", "")
            s.amount = p.get("amount", 0)
            s.tax_no = p.get("tax_no", "")
            s.status = INVOICE_REQUESTED
        elif e.event_type == ev.INVOICE_ISSUED:
            s.status = INVOICE_ISSUED
        elif e.event_type == ev.INVOICE_VOIDED:
            s.status = INVOICE_VOIDED
        elif e.event_type == ev.INVOICE_REISSUED:
            s.status = p.get("kind", INVOICE_CREDIT)
            s.related_invoice_id = p.get("related_invoice_id")
    return s


def _replay_arbitration(cid: str, events: list[ev.Event]) -> ArbitrationState:
    s = ArbitrationState(case_id=cid)
    for e in events:
        p = e.payload
        if e.event_type in (ev.ARBITRATION_OPENED, ev.ORDER_REMEDIED):
            s.order_id = p["order_id"]
            s.status = ARBITRATION_OPEN
            s.reason = p.get("reason", "")
        elif e.event_type == ev.ARBITRATION_RESOLVED:
            s.status = ARBITRATION_RESOLVED
            s.resolution = p.get("resolution", "")
            s.note = p.get("note", "")
    return s


def replay_eligibility(key: str, events: list[ev.Event]) -> EligibilityState | None:
    if not events:
        return None
    s = EligibilityState(key=key)
    for e in events:
        p = e.payload
        if e.event_type == ev.ELIGIBILITY_GRANTED:
            s.variant_id = p["variant_id"]
            s.user_id = p["user_id"]
            s.active = True
            s.grant_kind = p["grant_kind"]
            s.verified = p.get("verified", False)
            s.channel = p.get("channel", "")
        elif e.event_type == ev.ELIGIBILITY_REVOKED:
            s.active = False
    return s
