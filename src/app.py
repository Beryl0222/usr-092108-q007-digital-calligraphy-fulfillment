"""装配：事件存储 + 履约服务 + 读模型。"""

from __future__ import annotations

from dataclasses import dataclass

from .events import utc_now
from .projections import ReadModel
from .service import FulfilmentService
from .store import EventStore


@dataclass
class Application:
    store: EventStore
    service: FulfilmentService
    reads: ReadModel


def build_application(clock=utc_now) -> Application:
    store = EventStore()
    reads = ReadModel()
    # 投影在存储提交锁内同步更新，读到的状态永远不落后于事件。
    store.add_listener(reads.handle)
    service = FulfilmentService(store, clock=clock)
    return Application(store=store, service=service, reads=reads)
