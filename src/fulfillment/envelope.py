"""事件信封。

沿用仓库既有约定（event_id / event_type / aggregate_type / aggregate_id /
occurred_at / version / summary），在此之上补充：

* ``payload``：事件的结构化业务字段；
* ``causation_id``：触发本事件的入站请求（支付回调、物流回传等）外部标识，
  同一外部标识重复送达时据此保持幂等；
* ``correlation_id``：把一次下单牵动的数字、实体、发票等记录串起来；
* ``seq``：存储层分配的全局追加序号，只增不改。

事件一经追加即不可变；业务更正只能产生后继事件（追加状态纠正）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# 与 contracts/domain.schema.json 的 enum 保持同步；集中列出也供视图层使用。
EVENT_TYPES = (
    # 出版与配置
    "PROJECT_PUBLISHED",
    "VARIANT_CONFIGURED",
    "CLAIM_RULE_PUBLISHED",
    # 资格
    "ENTITLEMENT_GRANTED",
    # 序号池与订单生命周期
    "SERIAL_HELD",
    "HOLD_RELEASED",
    "SERIAL_ISOLATED",
    "ORDER_PLACED",
    "ORDER_CANCELLED",
    "PAYMENT_REGISTERED",
    "PAYMENT_SETTLED",
    "PAYMENT_REFUNDED",
    # 链上登记（不可删除，只追加纠正）
    "REGISTRATION_REQUESTED",
    "REGISTRATION_ACCEPTED",
    "REGISTRATION_CORRECTED",
    # 实体仓配
    "PHYSICAL_ALLOCATED",
    "PHYSICAL_PACKED",
    "PHYSICAL_DISPATCHED",
    "PHYSICAL_DELIVERED",
    "PHYSICAL_RETURNED",
    "PHYSICAL_LOST",
    "PHYSICAL_REPLACEMENT_ALLOCATED",
    "PHYSICAL_ISOLATED",
    # 撤销（超时/取消）
    "FULFILLMENT_REVOKED",
    # 票据
    "INVOICE_ISSUED",
    "INVOICE_VOIDED",
    # 仲裁
    "ARBITRATION_OPENED",
    "ARBITRATION_RESOLVED",
    "ORDER_REMEDIED",
)

AGGREGATE_TYPES = (
    "project",
    "edition",
    "serial_pool",
    "entitlement",
    "purchase_order",
    "registration",
    "physical_shipment",
    "invoice",
    "fulfillment_case",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    version: int
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)
    causation_id: str = ""
    correlation_id: str = ""
    seq: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "version": self.version,
            "summary": self.summary,
            "payload": self.payload,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "seq": self.seq,
        }


def envelope(
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    version: int,
    summary: str,
    payload: dict[str, Any] | None = None,
    *,
    event_id: str | None = None,
    occurred_at: str | None = None,
    causation_id: str = "",
    correlation_id: str = "",
) -> Event:
    """构造一条事件记录（尚未追加，因此没有全局 seq）。"""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"未知事件类型：{event_type}")
    if aggregate_type not in AGGREGATE_TYPES:
        raise ValueError(f"未知聚合类型：{aggregate_type}")
    if not isinstance(version, int) or version < 1:
        raise ValueError("version 必须是正整数")
    return Event(
        event_id=event_id or f"evt-{aggregate_id}-v{version}-{event_type.lower()}",
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=occurred_at or now_iso(),
        version=version,
        summary=summary,
        payload=dict(payload or {}),
        causation_id=causation_id,
        correlation_id=correlation_id,
    )
