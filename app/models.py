from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ChapterMeta:
    id: str
    title: str
    link: str
    raw_title: str = ""

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "ChapterMeta":
        return cls(
            id=str(data["id"]),
            title=str(data["title"]),
            link=str(data["link"]),
            raw_title=str(data.get("raw_title") or data["title"]),
        )

    def to_mapping(self) -> dict[str, str]:
        return {"id": self.id, "title": self.title, "link": self.link, "raw_title": self.raw_title or self.title}
