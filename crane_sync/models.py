"""核心数据模型与单位归一化。

巡检单间隙读数在供应商处可能混用毫米(mm)与微米(um)，
进入本地存储前统一换算为毫米。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Iterable

# 各单位到毫米的换算因子
_MM_FACTOR = {
    "mm": 1.0,
    "um": 0.001,   # 1 微米 = 0.001 毫米
    "μm": 0.001,
}


def normalize_gap(value: Any, unit: str | None) -> float:
    """把间隙读数统一换算为毫米。"""
    if value is None:
        raise ValueError("间隙读数不能为空")
    factor = _MM_FACTOR.get((unit or "mm").strip().lower())
    if factor is None:
        raise ValueError(f"不支持的间隙单位: {unit!r}")
    return round(float(value) * factor, 6)


def normalize_gaps(gaps: Iterable[dict]) -> list[dict]:
    """归一化一张单上的全部间隙读数，单位字段统一为 mm。"""
    out: list[dict] = []
    for g in gaps or []:
        out.append(
            {
                "point": g.get("point"),
                "value_mm": normalize_gap(g.get("value"), g.get("unit")),
                "unit": "mm",
            }
        )
    return out


@dataclass
class InspectionOrder:
    """一张某修订号的岸桥巡检单。"""

    order_id: str
    revision: int
    source: str
    inspected_at: str
    gaps: list[dict] = field(default_factory=list)
    position: int | None = None

    @classmethod
    def from_payload(cls, payload: dict, source: str) -> "InspectionOrder":
        return cls(
            order_id=str(payload["order_id"]),
            revision=int(payload["revision"]),
            source=source,
            inspected_at=str(payload["inspected_at"]),
            gaps=normalize_gaps(payload.get("gaps", [])),
        )

    def to_row(self) -> dict:
        return asdict(self)
