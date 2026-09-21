"""仅追加事件存储。

特性：
- append-only：不提供任何删除/改写接口，业务更正只能追加后继事件；
- 幂等：相同 ``event_id`` 重放直接返回，不产生第二条记录；
- 乐观并发：按聚合流版本校验，乱序提交抛 VersionConflict；
- 全局提交锁：多个聚合的联动提交在同一临界区完成，
  保证「占一个序号 + 建订单」在并发下不会多发一个编号。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable, Iterable

from .errors import VersionConflict
from .events import Event


class EventStore:
    def __init__(self) -> None:
        self._streams: dict[str, list[Event]] = defaultdict(list)
        self._by_event_id: dict[str, Event] = {}
        self._by_causation: dict[str, Event] = {}
        self._lock = threading.RLock()
        self._listeners: list[Callable[[Event], None]] = []

    def add_listener(self, listener: Callable[[Event], None]) -> None:
        with self._lock:
            self._listeners.append(listener)

    # ---- 读 --------------------------------------------------------------

    def stream(self, aggregate_id: str) -> list[Event]:
        with self._lock:
            return list(self._streams.get(aggregate_id, ()))

    def version(self, aggregate_id: str) -> int:
        with self._lock:
            return len(self._streams.get(aggregate_id, ()))

    def aggregate_ids(self, aggregate_type: str) -> list[str]:
        """列出某类型的全部聚合流（以流首事件的类型判定）。"""
        with self._lock:
            return [sid for sid, stream in self._streams.items()
                    if stream and stream[0].aggregate_type == aggregate_type]

    def all_events(self) -> list[Event]:
        with self._lock:
            events: list[Event] = []
            for stream in self._streams.values():
                events.extend(stream)
            events.sort(key=lambda e: (e.occurred_at, e.event_id))
            return events

    def has_event(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._by_event_id

    def get_event(self, event_id: str) -> Event | None:
        with self._lock:
            return self._by_event_id.get(event_id)

    def by_causation(self, causation_id: str) -> Event | None:
        """按外部回调/命令标识找回首次处理它的事件（幂等去重）。"""
        with self._lock:
            return self._by_causation.get(causation_id)

    # ---- 写 --------------------------------------------------------------

    def append(
        self,
        events: Iterable[Event],
        expected_versions: dict[str, int] | None = None,
    ) -> list[Event]:
        """追加一个或多个事件（可跨多个聚合流原子提交）。

        ``expected_versions`` 给出每个流提交前应有的长度。
        已存在的 event_id 视为幂等重放：要求同批其余 event_id 也全部已存在，
        然后原样返回，不重复通知监听者。
        """
        events = list(events)
        if not events:
            return []
        with self._lock:
            known = [self._by_event_id.get(e.event_id) for e in events]
            if all(k is not None for k in known):
                return list(known)  # type: ignore[arg-type]
            if any(k is not None for k in known):
                raise VersionConflict("批次中存在已写入的 event_id，提交不具原子性")

            for sid in {e.aggregate_id for e in events}:
                current = len(self._streams.get(sid, ()))
                if expected_versions is not None and sid in expected_versions:
                    if expected_versions[sid] != current:
                        raise VersionConflict(
                            f"聚合 {sid} 版本冲突：期望 {expected_versions[sid]}，实际 {current}"
                        )

            # 同一流内的事件必须按 1..n 连续编号；多流连号各自独立。
            per_stream: dict[str, list[Event]] = defaultdict(list)
            for e in events:
                per_stream[e.aggregate_id].append(e)
            for sid, grouped in per_stream.items():
                base = len(self._streams.get(sid, ()))
                for offset, e in enumerate(grouped, start=1):
                    if e.version != base + offset:
                        raise VersionConflict(
                            f"聚合 {sid} 事件版本号应为 {base + offset}，实际 {e.version}"
                        )

            # 先运行投影：任何监听器失败都中止提交，存储保持原样，
            # 调用方可安全重试（事件尚未落库，不会留下半提交状态）。
            for e in events:
                for listener in self._listeners:
                    listener(e)

            appended: list[Event] = []
            for e in events:
                self._streams[e.aggregate_id].append(e)
                self._by_event_id[e.event_id] = e
                if e.causation_id:
                    self._by_causation.setdefault(e.causation_id, e)
                appended.append(e)
            return appended

    def atomically(self, build: Callable[[], list[Event]]) -> list[Event]:
        """在全局提交锁内读流、决策并提交，``build`` 返回待追加事件。

        聚合的决策函数在锁内重放最新状态，杜绝两个并发请求同时看到
        「还有一个名额」而各自占号。
        """
        with self._lock:
            return build()
