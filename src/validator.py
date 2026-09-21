"""校验领域事件信封的基础字段。"""

from .events import AGGREGATE_TYPES, EVENT_TYPES

REQUIRED = ("event_id", "event_type", "aggregate_type", "aggregate_id",
            "occurred_at", "version", "summary")


def validate_event(record: dict) -> list[str]:
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if "version" in record and (not isinstance(record["version"], int)
                                or isinstance(record["version"], bool)
                                or record["version"] < 1):
        errors.append("version 必须是正整数")
    if record.get("event_type") and record["event_type"] not in EVENT_TYPES:
        errors.append(f"未知事件类型：{record['event_type']}")
    if record.get("aggregate_type") and record["aggregate_type"] not in AGGREGATE_TYPES:
        errors.append(f"未知聚合类型：{record['aggregate_type']}")
    return errors
