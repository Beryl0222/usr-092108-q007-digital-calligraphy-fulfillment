"""领域状态：从事件流折叠出的快照。

存储里只有不可变事件；任何状态判断都由这些 fold 函数现场推导，
因此不存在“改状态”的路径，只有“追加事件改变折叠结果”的路径。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .envelope import Event

# 订单/支付状态
PENDING = "pending"
PAID = "paid"
CANCELLED = "cancelled"
REFUNDED = "refunded"

# 序号状态
AVAILABLE = "available"
HELD = "held"
OWNED = "owned"
ISOLATED = "isolated"
RESERVED = "reserved"          # 实体套装已占号、订单待支付
COMMITTED = "committed"        # 已支付、进入仓配

# 登记状态（有效状态可被追加纠正改写，但历史事件保留）
REG_REQUESTED = "requested"
REG_ACCEPTED = "accepted"
REG_REVOKED = "revoked"

FREE = "free"
PAID_CHANNEL = "paid"


@dataclass
class Variant:
    variant_id: str
    name: str
    kind: str           # digital_paid | digital_free
    cap: int
    price: int          # 分为单位
    pool_id: str


@dataclass
class Project:
    project_id: str
    name: str
    physical_cap: int
    physical_pool_id: str
    variants: dict[str, Variant] = field(default_factory=dict)
    claim_rule: dict[str, Any] = field(default_factory=dict)


@dataclass
class PaymentRecord:
    external_payment_id: str
    amount: int
    status: str                 # registered | succeeded | failed
    refunded: bool = False
    refund_reason: str = ""


@dataclass
class Order:
    order_id: str
    project_id: str
    variant_id: str
    user_id: str
    channel: str
    price: int
    serial_no: str = ""
    with_physical: bool = False
    collectible_no: str | None = None
    grant_id: str | None = None
    hold_expires_at: str = ""
    status: str = PENDING
    cancel_reason: str = ""
    payments: list[PaymentRecord] = field(default_factory=list)
    settled_amount: int = 0
    refund: dict[str, Any] | None = None
    registration_id: str | None = None
    invoice_id: str | None = None
    shipment_id: str | None = None
    version: int = 0


@dataclass
class SerialLedger:
    """一个款式序号池的折叠结果，键为序号。"""

    pool_id: str
    cap: int
    # serial -> 当前占有人/订单的折叠记录
    serials: dict[str, dict[str, Any]] = field(default_factory=dict)

    def status_of(self, serial: str) -> str:
        entry = self.serials.get(serial)
        return entry["status"] if entry else AVAILABLE


@dataclass
class Shipment:
    shipment_id: str
    order_id: str
    collectible_no: str
    attempts: list[dict[str, Any]] = field(default_factory=list)
    state: str = "allocated"     # allocated|packed|dispatched|delivered|lost|returned
    version: int = 0

    @property
    def current_attempt(self) -> dict[str, Any] | None:
        return self.attempts[-1] if self.attempts else None


@dataclass
class Registration:
    registration_id: str
    order_id: str
    serial_no: str
    state: str = REG_REQUESTED
    token_id: str = ""
    corrections: list[dict[str, Any]] = field(default_factory=list)
    version: int = 0

    @property
    def effective(self) -> dict[str, Any]:
        """接受登记 + 历次纠正后的当前有效内容（历史仍完整保留）。"""
        view: dict[str, Any] = {"state": self.state, "token_id": self.token_id}
        for patch in self.corrections:
            view.update(patch.get("changes", {}))
        return view


@dataclass
class Invoice:
    invoice_id: str
    order_id: str
    amount: int
    voided: bool = False
    version: int = 0


@dataclass
class Case:
    case_id: str
    order_id: str
    reason: str
    ruling: str = ""
    remedies: list[dict[str, Any]] = field(default_factory=list)
    open: bool = True
    version: int = 0


def fold_project(events: list[Event]) -> Project | None:
    project: Project | None = None
    for e in events:
        if e.event_type == "PROJECT_PUBLISHED":
            project = Project(
                project_id=e.aggregate_id,
                name=e.payload["name"],
                physical_cap=e.payload["physical_cap"],
                physical_pool_id=e.payload["physical_pool_id"],
            )
        elif e.event_type == "CLAIM_RULE_PUBLISHED":
            assert project is not None
            project.claim_rule = e.payload
    return project


def fold_variant(events: list[Event]) -> Variant | None:
    for e in events:
        if e.event_type == "VARIANT_CONFIGURED":
            p = e.payload
            return Variant(
                variant_id=e.aggregate_id,
                name=p["name"],
                kind=p["kind"],
                cap=p["cap"],
                price=p["price"],
                pool_id=p["pool_id"],
            )
    return None


def fold_order(events: list[Event]) -> Order | None:
    order: Order | None = None
    for e in events:
        p = e.payload
        if e.event_type == "ORDER_PLACED":
            order = Order(
                order_id=e.aggregate_id,
                project_id=p["project_id"],
                variant_id=p["variant_id"],
                user_id=p["user_id"],
                channel=p["channel"],
                price=p["price"],
                serial_no=p["serial_no"],
                with_physical=p.get("with_physical", False),
                collectible_no=p.get("collectible_no"),
                grant_id=p.get("grant_id"),
                hold_expires_at=p["hold_expires_at"],
            )
        elif order is None:
            continue
        elif e.event_type == "PAYMENT_REGISTERED":
            order.payments.append(
                PaymentRecord(p["external_payment_id"], p["amount"], p.get("status", "registered"))
            )
        elif e.event_type == "PAYMENT_SETTLED":
            order.status = PAID
            order.settled_amount = p["amount"]
            for pay in order.payments:
                if pay.external_payment_id == p.get("external_payment_id"):
                    pay.status = "succeeded"
        elif e.event_type == "ORDER_CANCELLED":
            order.status = CANCELLED
            order.cancel_reason = p.get("reason", "")
        elif e.event_type == "PAYMENT_REFUNDED":
            order.status = REFUNDED
            order.refund = p
            for pay in order.payments:
                if pay.external_payment_id == p.get("external_payment_id"):
                    pay.refunded = True
                    pay.refund_reason = p.get("reason", "")
        elif e.event_type == "FULFILLMENT_REVOKED":
            order.status = REFUNDED
            order.cancel_reason = p.get("reason", order.cancel_reason)
        elif e.event_type == "INVOICE_ISSUED":
            order.invoice_id = e.aggregate_id
    if order is not None:
        order.version = events[-1].version if events else 0
    return order


def fold_serial_pool(events: list[Event], pool_id: str, cap: int) -> SerialLedger:
    """数字序号池：SERIAL_HELD / HOLD_RELEASED 构成显式占用台账。"""
    ledger = SerialLedger(pool_id=pool_id, cap=cap)
    for e in events:
        p = e.payload
        if e.event_type == "SERIAL_HELD":
            ledger.serials[p["serial_no"]] = {
                "status": HELD,
                "order_id": p["order_id"],
                "user_id": p["user_id"],
                "held_at": e.occurred_at,
                "expires_at": p.get("expires_at"),
                "releases": ledger.serials.get(p["serial_no"], {}).get("releases", []),
            }
        elif e.event_type == "HOLD_RELEASED":
            entry = ledger.serials.setdefault(
                p["serial_no"], {"releases": []}
            )
            entry["status"] = AVAILABLE
            entry["order_id"] = None
            entry["user_id"] = None
            entry.setdefault("releases", []).append(
                {"reason": p.get("reason", ""), "at": e.occurred_at, "causation": e.causation_id}
            )
        elif e.event_type == "SERIAL_ISOLATED":
            entry = ledger.serials.setdefault(p["serial_no"], {"releases": []})
            entry["status"] = ISOLATED
            entry["isolated_reason"] = p.get("reason", "")
            entry["isolated_at"] = e.occurred_at
    return ledger


def fold_physical_pool(events: list[Event], pool_id: str, cap: int) -> SerialLedger:
    """实体套装池：占号（预留）、释放（未支付）、隔离（撤销/异常）。"""
    ledger = SerialLedger(pool_id=pool_id, cap=cap)
    for e in events:
        p = e.payload
        if e.event_type == "PHYSICAL_ALLOCATED":
            ledger.serials[p["collectible_no"]] = {
                "status": RESERVED,
                "order_id": p["order_id"],
                "user_id": p["user_id"],
                "held_at": e.occurred_at,
            }
        elif e.event_type == "HOLD_RELEASED":
            entry = ledger.serials.setdefault(p["collectible_no"], {})
            entry["status"] = AVAILABLE
            entry["order_id"] = None
            entry["user_id"] = None
            entry.setdefault("releases", []).append(
                {"reason": p.get("reason", ""), "at": e.occurred_at}
            )
        elif e.event_type == "PHYSICAL_ISOLATED":
            entry = ledger.serials.setdefault(p["collectible_no"], {})
            entry["status"] = ISOLATED
            entry["isolated_reason"] = p.get("reason", "")
            entry["isolated_at"] = e.occurred_at
    return ledger


def fold_shipment(events: list[Event]) -> Shipment | None:
    shipment: Shipment | None = None
    for e in events:
        p = e.payload
        if e.event_type == "PHYSICAL_PACKED":
            shipment = Shipment(
                shipment_id=e.aggregate_id,
                order_id=p["order_id"],
                collectible_no=p["collectible_no"],
            )
            shipment.attempts.append({"attempt": p.get("attempt", 1), "state": "packed", "packed_at": e.occurred_at})
            shipment.state = "packed"
        elif shipment is None:
            continue
        elif e.event_type == "PHYSICAL_DISPATCHED":
            shipment.attempts[-1].update({"state": "dispatched", "carrier": p.get("carrier", ""),
                                          "waybill_no": p.get("waybill_no", ""), "dispatched_at": e.occurred_at})
            shipment.state = "dispatched"
        elif e.event_type == "PHYSICAL_DELIVERED":
            shipment.attempts[-1].update({"state": "delivered", "delivered_at": e.occurred_at})
            shipment.state = "delivered"
        elif e.event_type == "PHYSICAL_RETURNED":
            shipment.attempts[-1].update({"state": "returned", "returned_at": e.occurred_at})
            shipment.state = "returned"
        elif e.event_type == "PHYSICAL_LOST":
            shipment.attempts[-1].update({"state": "lost", "lost_at": e.occurred_at, "proof": p.get("proof", "")})
            shipment.state = "lost"
        elif e.event_type == "PHYSICAL_REPLACEMENT_ALLOCATED":
            shipment.attempts.append({"attempt": p.get("attempt", len(shipment.attempts) + 1),
                                      "state": "packed", "collectible_no": shipment.collectible_no,
                                      "packed_at": e.occurred_at, "replacement_for": p.get("reason", "lost")})
            shipment.state = "packed"
    if shipment is not None:
        shipment.version = events[-1].version if events else 0
    return shipment


def fold_registration(events: list[Event]) -> Registration | None:
    reg: Registration | None = None
    for e in events:
        p = e.payload
        if e.event_type == "REGISTRATION_REQUESTED":
            reg = Registration(
                registration_id=e.aggregate_id, order_id=p["order_id"], serial_no=p["serial_no"]
            )
        elif reg is None:
            continue
        elif e.event_type == "REGISTRATION_ACCEPTED":
            reg.state = REG_ACCEPTED
            reg.token_id = p.get("token_id", "")
        elif e.event_type == "REGISTRATION_CORRECTED":
            reg.corrections.append({"changes": p.get("changes", {}), "reason": p.get("reason", ""),
                                    "at": e.occurred_at, "case_id": p.get("case_id", "")})
            if p.get("changes", {}).get("state") == REG_REVOKED:
                reg.state = REG_REVOKED
    if reg is not None:
        reg.version = events[-1].version if events else 0
    return reg


def fold_invoice(events: list[Event]) -> Invoice | None:
    invoice: Invoice | None = None
    for e in events:
        if e.event_type == "INVOICE_ISSUED":
            invoice = Invoice(invoice_id=e.aggregate_id, order_id=e.payload["order_id"],
                              amount=e.payload["amount"])
        elif e.event_type == "INVOICE_VOIDED" and invoice is not None:
            invoice.voided = True
    if invoice is not None:
        invoice.version = events[-1].version if events else 0
    return invoice


def fold_case(events: list[Event]) -> Case | None:
    case: Case | None = None
    for e in events:
        p = e.payload
        if e.event_type == "ARBITRATION_OPENED":
            case = Case(case_id=e.aggregate_id, order_id=p["order_id"], reason=p.get("reason", ""))
        elif case is None:
            continue
        elif e.event_type == "ORDER_REMEDIED":
            case.remedies.append(p)
        elif e.event_type == "ARBITRATION_RESOLVED":
            case.open = False
            case.ruling = p.get("ruling", "")
    if case is not None:
        case.version = events[-1].version if events else 0
    return case
