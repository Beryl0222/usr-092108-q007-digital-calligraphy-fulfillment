"""事件信封。

所有跨系统消息（支付、链上登记、仓配物流、发票、仲裁）都统一使用本信封：
事件一旦写入即不可变，任何业务更正都通过追加后继事件完成。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Any

# ---- 聚合类型 -------------------------------------------------------------

AGG_PROJECT = "edition_project"        # 出版项目（一次发行）
AGG_VARIANT = "variant"                # 款式（四款各 1000 / 免费版 / 实体套）
AGG_ELIGIBILITY = "eligibility"        # 用户对某款式的领取/购买资格
AGG_ORDER = "purchase_order"           # 订单（保留、支付、退款）
AGG_REGISTRY = "registration_ledger"   # 链上登记追加账（每款式一本）
AGG_FULFILLMENT = "fulfillment_case"   # 实体仓配履约单
AGG_INVOICE = "invoice"                # 发票
AGG_CASE = "arbitration_case"          # 仲裁/客服工单

# 兼容旧信封里的取值
AGG_LEGACY_EDITION = "edition"
AGG_LEGACY_HOLD = "serial_reservation"

AGGREGATE_TYPES = frozenset({
    AGG_PROJECT, AGG_VARIANT, AGG_ELIGIBILITY, AGG_ORDER, AGG_REGISTRY,
    AGG_FULFILLMENT, AGG_INVOICE, AGG_CASE,
    AGG_LEGACY_EDITION, AGG_LEGACY_HOLD,
})

# ---- 事件类型（全部为已发生的事实，名称用过去式） ---------------------------

# 项目与款式
PROJECT_CREATED = "PROJECT_CREATED"
VARIANT_OPENED = "VARIANT_OPENED"
VARIANT_CLOSED = "VARIANT_CLOSED"

# 资格
ELIGIBILITY_GRANTED = "ELIGIBILITY_GRANTED"          # 发放资格（白名单/活动）
ELIGIBILITY_REVOKED = "ELIGIBILITY_REVOKED"

# 下单与名额占用
SERIAL_HELD = "SERIAL_HELD"                          # 保留序号（兼容旧名）
HOLD_EXTENDED = "HOLD_EXTENDED"
HOLD_RELEASED = "HOLD_RELEASED"                      # 超时/取消释放

# 订单
ORDER_PLACED = "ORDER_PLACED"
ORDER_CANCELLED = "ORDER_CANCELLED"
PAYMENT_CONFIRMED = "PAYMENT_CONFIRMED"              # 兼容旧名
PAYMENT_LATE_REJECTED = "PAYMENT_LATE_REJECTED"      # 迟到支付：不再成交
REFUND_GRANTED = "REFUND_GRANTED"

# 链上登记（追加式，不可删改，纠正只能再追加）
REGISTRATION_REQUESTED = "REGISTRATION_REQUESTED"
REGISTRATION_ACCEPTED = "REGISTRATION_ACCEPTED"      # 兼容旧名
REGISTRATION_FAILED = "REGISTRATION_FAILED"
REGISTRATION_CORRECTED = "REGISTRATION_CORRECTED"    # 追加纠正（不覆盖原记录）

# 实体仓配
PHYSICAL_ALLOCATED = "PHYSICAL_ALLOCATED"            # 实体件按收藏序号配号
PHYSICAL_QUARANTINED = "PHYSICAL_QUARANTINED"        # 未发实体随撤销隔离
PHYSICAL_DISPATCHED = "PHYSICAL_DISPATCHED"          # 兼容旧名
PHYSICAL_DELIVERED = "PHYSICAL_DELIVERED"            # 签收
PHYSICAL_LOST = "PHYSICAL_LOST"                      # 物流判丢
PHYSICAL_RESENT = "PHYSICAL_RESENT"                  # 补发，沿用原收藏序号
PHYSICAL_RETURNED = "PHYSICAL_RETURNED"

# 发票
INVOICE_REQUESTED = "INVOICE_REQUESTED"
INVOICE_ISSUED = "INVOICE_ISSUED"
INVOICE_VOIDED = "INVOICE_VOIDED"
INVOICE_REISSUED = "INVOICE_REISSUED"

# 仲裁
ORDER_REMEDIED = "ORDER_REMEDIED"                    # 兼容旧名
ARBITRATION_OPENED = "ARBITRATION_OPENED"
ARBITRATION_RESOLVED = "ARBITRATION_RESOLVED"

EVENT_TYPES = frozenset({
    PROJECT_CREATED, VARIANT_OPENED, VARIANT_CLOSED,
    ELIGIBILITY_GRANTED, ELIGIBILITY_REVOKED,
    SERIAL_HELD, HOLD_EXTENDED, HOLD_RELEASED,
    ORDER_PLACED, ORDER_CANCELLED, PAYMENT_CONFIRMED, PAYMENT_LATE_REJECTED,
    REFUND_GRANTED,
    REGISTRATION_REQUESTED, REGISTRATION_ACCEPTED, REGISTRATION_FAILED,
    REGISTRATION_CORRECTED,
    PHYSICAL_ALLOCATED, PHYSICAL_QUARANTINED, PHYSICAL_DISPATCHED,
    PHYSICAL_DELIVERED, PHYSICAL_LOST, PHYSICAL_RESENT, PHYSICAL_RETURNED,
    INVOICE_REQUESTED, INVOICE_ISSUED, INVOICE_VOIDED, INVOICE_REISSUED,
    ORDER_REMEDIED, ARBITRATION_OPENED, ARBITRATION_RESOLVED,
})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    """不可变事件信封。payload 只放完成职责所必需的字段。"""

    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: datetime
    version: int                    # 该事件在所属聚合流上的版本号（从 1 开始）
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)
    causation_id: str | None = None    # 触发本事件的外部回调/命令标识
    correlation_id: str | None = None  # 同一业务过程串联（一般是订单号）

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": _iso(self.occurred_at),
            "version": self.version,
            "summary": self.summary,
            "payload": self.payload,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
        }


class EventIdPolicy:
    """按聚合流生成确定性事件标识，便于回调重放时做幂等。"""

    def __init__(self) -> None:
        self._seq = count(1)

    def new(self, aggregate_id: str, kind: str, causation_id: str | None = None) -> str:
        if causation_id:
            return f"{aggregate_id}:{kind}:{causation_id}"
        return f"{aggregate_id}:{kind}:{next(self._seq)}"
