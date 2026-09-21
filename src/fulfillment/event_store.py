"""仅追加的事件存储。

内存实现（生产中可换成同一语义的事务型事件表）：

* 事件只能 append，从不修改或删除；
* ``event_id`` 全局唯一；同一 ``causation_id`` 的同类型事件只生效一次，
  重复送达返回首次结果——这是支付回调、物流回传等外部系统幂等的落点；
* 每个聚合的版本号严格 +1，构成乐观并发屏障；
* 一次业务决策涉及多个聚合时用 :meth:`EventStore.transaction` 原子提交。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Callable, Iterable, TypeVar

from .envelope import Event
from .errors import Conflict, NotFound

T = TypeVar("T")


class EventStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._log: list[Event] = []
        self._by_aggregate: dict[tuple[str, str], list[Event]] = defaultdict(list)
        self._event_ids: dict[str, Event] = {}
        # causation 维度的幂等索引：(causation_id, aggregate_id) -> 已提交事件
        self._by_causation: dict[tuple[str, str], list[Event]] = defaultdict(list)

    # ---- 读 ----------------------------------------------------------------

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[Event]:
        with self._lock:
            return list(self._by_aggregate[(aggregate_type, aggregate_id)])

    def stream(self, *, event_type: str | None = None) -> list[Event]:
        with self._lock:
            if event_type is None:
                return list(self._log)
            return [e for e in self._log if e.event_type == event_type]

    def version_of(self, aggregate_type: str, aggregate_id: str) -> int:
        with self._lock:
            history = self._by_aggregate[(aggregate_type, aggregate_id)]
            return history[-1].version if history else 0

    def get(self, aggregate_type: str, aggregate_id: str) -> list[Event]:
        history = self.events_for(aggregate_type, aggregate_id)
        if not history:
            raise NotFound(f"{aggregate_type}/{aggregate_id} 不存在")
        return history

    def causation_events(self, causation_id: str, aggregate_id: str) -> list[Event]:
        with self._lock:
            return list(self._by_causation.get((causation_id, aggregate_id), ()))

    def find_by_causation(self, causation_id: str) -> list[Event]:
        """按外部请求标识反查全部已提交事件（下单幂等键在生成订单号前使用）。"""
        with self._lock:
            found: list[Event] = []
            for (cid, _agg), events in self._by_causation.items():
                if cid == causation_id:
                    found.extend(events)
            return found

    # ---- 写 ----------------------------------------------------------------

    def append(self, event: Event, *, expected_version: int) -> Event:
        """追加单条事件并做版本校验。"""
        with self._lock:
            return self._append_locked(event, expected_version)

    def append_many(self, events: Iterable[Event], *, expected_versions: dict[tuple[str, str], int]) -> None:
        """原子追加多条跨聚合事件；任一校验失败则整体不生效。

        ``expected_versions`` 给出各聚合在本批次开始前的基线版本；同一聚合
        在批次内出现多条时，版本必须沿基线连续 +1。
        """
        events = list(events)
        with self._lock:
            position: dict[tuple[str, str], int] = {}
            for event in events:
                key = (event.aggregate_type, event.aggregate_id)
                baseline = expected_versions.get(key, 0)
                step = position.get(key, 0)
                if event.event_id in self._event_ids:
                    raise Conflict(f"event_id 已存在：{event.event_id}")
                history = self._by_aggregate[key]
                current = history[-1].version if history else 0
                if current != baseline:
                    raise Conflict(
                        f"{event.aggregate_type}/{event.aggregate_id} 版本冲突：期望基线 {baseline}，实际 {current}"
                    )
                wanted = baseline + step + 1
                if event.version != wanted:
                    raise Conflict(f"新版本必须为 {wanted}，收到 {event.version}")
                position[key] = step + 1
            for event in events:
                self._commit_locked(event)

    def transaction(self, fn: Callable[[], T]) -> T:
        """在同一把存储锁内完成 读-决策-写，串行化所有业务命令。

        领域不变量（不超发、不重号）由此获得单进程内的可线性化保证；
        换成数据库时对应“单写者/select for update 序号池行”的事务边界。
        """
        with self._lock:
            return fn()

    # ---- 内部 --------------------------------------------------------------

    def _check_locked(self, event: Event, expected_version: int) -> None:
        if event.event_id in self._event_ids:
            raise Conflict(f"event_id 已存在：{event.event_id}")
        history = self._by_aggregate[(event.aggregate_type, event.aggregate_id)]
        current = history[-1].version if history else 0
        if current != expected_version:
            raise Conflict(
                f"{event.aggregate_type}/{event.aggregate_id} 版本冲突：期望 {expected_version}，实际 {current}"
            )
        if event.version != current + 1:
            raise Conflict(f"新版本必须为 {current + 1}，收到 {event.version}")

    def _append_locked(self, event: Event, expected_version: int) -> Event:
        self._check_locked(event, expected_version)
        return self._commit_locked(event)

    def _commit_locked(self, event: Event) -> Event:
        seq = len(self._log) + 1
        stored = Event(**{**event.to_dict(), "seq": seq})
        self._log.append(stored)
        self._by_aggregate[(stored.aggregate_type, stored.aggregate_id)].append(stored)
        self._event_ids[stored.event_id] = stored
        if stored.causation_id:
            self._by_causation[(stored.causation_id, stored.aggregate_id)].append(stored)
        return stored
