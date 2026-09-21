"""《草书诗帖》发行项目的标准建档引导。

四款各一千份的付费数字作品、一个免费限领版本、一千套实体组合。
业务方也可以用底层命令自行建档；这里只固化发行口径。
"""

from __future__ import annotations

from .services import FulfillmentService

PROJECT_ID = "CAOSHU_SHITIE"

# 四款付费数字作品（单位：分）
PAID_VARIANTS = (
    ("CS-01", "草书诗帖·第一帖", 19900),
    ("CS-02", "草书诗帖·第二帖", 19900),
    ("CS-03", "草书诗帖·第三帖", 19900),
    ("CS-04", "草书诗帖·第四帖", 19900),
)
# 免费限领版本
FREE_VARIANT = ("CS-FREE", "草书诗帖·免费限领版")

DIGITAL_CAP = 1000
FREE_CAP = 1000
PHYSICAL_CAP = 1000


def bootstrap_edition(service: FulfillmentService, *, project_id: str = PROJECT_ID) -> str:
    service.publish_project(project_id, "草书诗帖", physical_cap=PHYSICAL_CAP)
    for variant_id, name, price in PAID_VARIANTS:
        service.configure_variant(
            project_id, variant_id, name,
            kind="digital_paid", cap=DIGITAL_CAP, price=price,
        )
    free_id, free_name = FREE_VARIANT
    service.configure_variant(
        project_id, free_id, free_name, kind="digital_free", cap=FREE_CAP, price=0
    )
    service.publish_claim_rule(project_id, free_variant_ids=[free_id])
    return project_id
