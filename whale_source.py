"""whale 日记与事件流的读取层。

把 `whale-diary.md` 与 `whale-diary-private.md` 按「日期 + 章节」切分：
- `给主人的信`：散文/情绪/原话；
- `看过`：Markdown 表格，保留 BV、标题、她的想法、评分；
- `想说但还没说`：未完成的分享意图；
- `偏好校准` / `补记`：私密日记内容，visibility=private。

这一层不碰 A_Memorix，也不依赖 MaiBot 运行时，方便单独测试。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set

# whale 日记里的 BV 号形如 BV1xxxxxxxxx（10 位大小写字母数字）。
_BV_PATTERN = re.compile(r"BV[0-9A-Za-z]{10}")
# 表格行里的日期写成 YYYY-MM-DD（whale 用 sv-SE 的本地日期）。
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
# 表格分隔行（| --- | --- |）。
_SEPARATOR_PATTERN = re.compile(r"^\|[\s:|-]+\|?$")
# 日期标题：`## 2026-10-19 · 看的那晚（23:47）`
_DAY_PATTERN = re.compile(r"^##\s+(\d{4}-\d{2}-\d{2})(?:\s*[·-]\s*(.*))?$")
_HEADING_PATTERN = re.compile(r"^(#{2,3})\s+(.+)$")

SECTION_LABELS: Dict[str, str] = {
    "letter": "给主人的信",
    "seen": "看过",
    "thoughts": "想说但还没说",
    "preference": "偏好校准",
    "private_note": "私密补记",
    "event": "事件",
}


@dataclass(frozen=True)
class WhaleMemoryItem:
    """一条待写入 A_Memorix 的 whale 记忆。"""

    external_id: str
    observed_at: str
    bv: str
    title: str
    thought: str
    section: str = "seen"
    entry_label: str = ""
    raw_text: str = ""
    score: str = ""
    visibility: str = "private"

    def render_text(self) -> str:
        """把条目渲染成一段可检索的纯文本。"""

        if self.section == "seen":
            parts = [self.observed_at or "日期未知"]
            if self.bv:
                parts.append(self.bv)
            if self.title:
                parts.append(self.title)
            if self.thought:
                parts.append(f"她的想法：{self.thought}")
            return "｜".join(parts)

        header = [self.observed_at or "日期未知"]
        if self.entry_label:
            header.append(self.entry_label)
        header.append(SECTION_LABELS.get(self.section, self.section))
        body = (self.raw_text or self.thought).strip()
        return "｜".join(header) + (f"\n{body}" if body else "")


def expand_path(value: str, fallback_home: Optional[Path] = None) -> Path:
    """展开配置里的路径（支持 ~ 与环境变量）。"""

    text = str(value or "").strip()
    if not text:
        raise ValueError("路径不能为空")
    return Path(os.path.expandvars(os.path.expanduser(text)))


def _split_cells(line: str) -> List[str]:
    """把 Markdown 表格行拆成单元格；首尾的空单元来自行首/行尾的竖线。"""

    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in text.split("|")]


def _is_header_row(cells: Sequence[str]) -> bool:
    """表头：含「标题/想法/BV/日期」等词，且没有真实日期或 BV 号。"""

    text = "|".join(cells)
    if _DATE_PATTERN.search(text) or _BV_PATTERN.search(text):
        return False
    return any(keyword in text for keyword in ("标题", "想法", "BV", "日期", "时间"))


def _header_index(header: Sequence[str], *names: str) -> Optional[int]:
    """按列名找下标：先完全匹配，再包含匹配。"""

    for name in names:
        for index, cell in enumerate(header):
            if cell.strip() == name:
                return index
    for name in names:
        for index, cell in enumerate(header):
            if name in cell:
                return index
    return None


def _cell_at(cells: Sequence[str], index: Optional[int]) -> str:
    """按列下标取值，越界返回空串。"""

    if index is None or index >= len(cells):
        return ""
    return str(cells[index]).strip()


def _priority_text(cells: Sequence[str], excluded: Set[str]) -> List[str]:
    """按长度从长到短返回剩余的文本单元（长的更像标题/想法）。"""

    remaining = [cell for cell in cells if cell not in excluded and cell]
    return sorted(remaining, key=len, reverse=True)


def _extract_score(text: str) -> str:
    """从「18/20」这类文本里提取评分。"""

    match = re.search(r"(\d{1,2})\s*/\s*20", text or "")
    return match.group(1) + "/20" if match else ""


def _short_hash(text: str) -> str:
    """内容短哈希，用于区分同一天同章节的不同版本。"""

    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:8]


def _entry_slug(label: str) -> str:
    """把日期标题里的标签变成可读的短 slug。"""

    text = re.sub(r"[^\w\u4e00-\u9fff]+", "_", (label or "").strip())
    return (text.strip("_")[:24] or "entry")


def _section_kind(title: str) -> Optional[str]:
    """把章节标题映射成内部类型。"""

    text = (title or "").strip()
    if "给主人的信" in text:
        return "letter"
    if text.startswith("看过"):
        return "seen"
    if "想说但还没说" in text:
        return "thoughts"
    if "偏好校准" in text:
        return "preference"
    if "补记" in text:
        return "private_note"
    return None


def _row_to_item(
    header: Sequence[str],
    cells: Sequence[str],
    *,
    default_date: str = "",
    entry_label: str = "",
    visibility: str = "private",
) -> Optional[WhaleMemoryItem]:
    """把一行表格转成记忆条目；优先按表头列名取值，缺表头时退回位置启发式。"""

    date_index = _header_index(header, "日期") if header else None
    time_index = _header_index(header, "时间") if header else None
    bv_index = _header_index(header, "BV") if header else None
    title_index = _header_index(header, "标题") if header else None
    thought_index = _header_index(header, "她的想法", "想法", "感想", "她的话") if header else None

    observed_at = _cell_at(cells, date_index) or _cell_at(cells, time_index) or default_date
    date_match = _DATE_PATTERN.search(observed_at)
    observed_at = date_match.group(0) if date_match else ""

    bv = _cell_at(cells, bv_index)
    bv_match = _BV_PATTERN.search(bv)
    bv = bv_match.group(0) if bv_match else ""

    title = _cell_at(cells, title_index)
    thought = _cell_at(cells, thought_index)

    if not title and not thought:
        # 表头缺失或列名变化：退回原来的“最长文本”启发式。
        all_date = next((_DATE_PATTERN.search(cell) for cell in cells if _DATE_PATTERN.search(cell)), None)
        all_bv = next((_BV_PATTERN.search(cell) for cell in cells if _BV_PATTERN.search(cell)), None)
        observed_at = observed_at or (all_date.group(0) if all_date else "")
        bv = bv or (all_bv.group(0) if all_bv else "")
        excluded = {cell for cell in cells if cell == (all_date.group(0) if all_date else "")}
        excluded.update(cell for cell in cells if cell == (all_bv.group(0) if all_bv else ""))
        texts = _priority_text(cells, excluded)
        thought = texts[0] if len(texts) >= 2 else ""
        title = texts[1] if len(texts) >= 2 else (texts[0] if texts else "")

    # 占位行（—/空）或没有日期也没有 BV 的非视频行，不进入长期记忆。
    if not title and not thought:
        return None
    if not observed_at and not bv:
        return None

    identity = bv or hashlib.sha1(f"{title}|{thought}".encode("utf-8")).hexdigest()[:12]
    external_id = f"whale:seen:{observed_at or 'unknown'}:{identity}"
    return WhaleMemoryItem(
        external_id=external_id,
        observed_at=observed_at,
        bv=bv,
        title=title,
        thought=thought,
        section="seen",
        entry_label=entry_label,
        raw_text=thought,
        score=_extract_score(thought),
        visibility=visibility,
    )


def _parse_table_rows(
    table_lines: Sequence[str],
    *,
    default_date: str,
    entry_label: str,
    visibility: str,
) -> List[WhaleMemoryItem]:
    """解析某个「看过」小节里的表格行。"""

    items: List[WhaleMemoryItem] = []
    header: List[str] = []
    for line in table_lines:
        stripped = line.strip()
        if not stripped.startswith("|") or _SEPARATOR_PATTERN.match(stripped):
            continue
        cells = _split_cells(stripped)
        if _is_header_row(cells):
            header = cells
            continue
        if not (_DATE_PATTERN.search(stripped) or _BV_PATTERN.search(stripped)):
            continue
        item = _row_to_item(
            header,
            cells,
            default_date=default_date,
            entry_label=entry_label,
            visibility=visibility,
        )
        if item is not None:
            items.append(item)
    return items


def parse_diary(path: Path, *, visibility: str = "private") -> List[WhaleMemoryItem]:
    """按「日期 + 章节」解析日记；默认视为私密内容。"""

    if not path.exists():
        return []
    items: List[WhaleMemoryItem] = []
    current_date = ""
    current_label = ""
    current_kind: Optional[str] = None
    buffer: List[str] = []
    table_lines: List[str] = []

    def flush() -> None:
        nonlocal buffer, table_lines
        if current_kind == "seen":
            items.extend(
                _parse_table_rows(
                    table_lines,
                    default_date=current_date,
                    entry_label=current_label,
                    visibility=visibility,
                )
            )
        elif current_kind in {"letter", "thoughts", "preference", "private_note"}:
            text = "\n".join(buffer).strip()
            if text:
                external_id = (
                    f"whale:{visibility}:{current_kind}:{current_date or 'unknown'}:"
                    f"{_entry_slug(current_label)}:{_short_hash(text)}"
                )
                items.append(
                    WhaleMemoryItem(
                        external_id=external_id,
                        observed_at=current_date,
                        bv="",
                        title=SECTION_LABELS.get(current_kind, current_kind),
                        thought=text,
                        section=current_kind,
                        entry_label=current_label,
                        raw_text=text,
                        score=_extract_score(text),
                        visibility=visibility,
                    )
                )
        buffer = []
        table_lines = []

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        day_match = _DAY_PATTERN.match(stripped)
        if day_match:
            flush()
            current_date = day_match.group(1)
            current_label = (day_match.group(2) or "").strip()
            # 私密日记的日期标题下常直接跟正文，先按「私密补记」收集；
            # 公开日记通常紧跟 `### 给主人的信`，这里设为 None 避免空段落。
            current_kind = "private_note" if visibility == "private" else None
            continue

        heading = _HEADING_PATTERN.match(stripped)
        if heading:
            kind = _section_kind(heading.group(2))
            flush()
            current_kind = kind
            continue

        if current_kind == "seen":
            if stripped.startswith("|"):
                table_lines.append(stripped)
        elif current_kind:
            buffer.append(line)

    flush()
    return items


def parse_private_diary(path: Path) -> List[WhaleMemoryItem]:
    """解析私密日记，visibility 固定为 private。"""

    return parse_diary(path, visibility="private")


def parse_event_log(path: Path, kinds: Set[str]) -> List[WhaleMemoryItem]:
    """把 whale-log.jsonl 里指定 kind 的事件转成记忆条目。"""

    if not path.exists() or not kinds:
        return []
    items: List[WhaleMemoryItem] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        kind = str(event.get("kind") or "").strip()
        if kind not in kinds:
            continue
        note = str(event.get("note") or "").strip()
        target = str(event.get("target") or "").strip()
        if not note and not target:
            continue
        observed_at = str(event.get("at") or "")[:10]
        identity = hashlib.sha1(f"{target}|{note}".encode("utf-8")).hexdigest()[:12]
        items.append(
            WhaleMemoryItem(
                external_id=f"whale:event:{observed_at}:{identity}",
                observed_at=observed_at,
                bv=target if _BV_PATTERN.fullmatch(target) else "",
                title=note or target,
                thought="",
                section="event",
                raw_text=note or target,
                visibility="private",
            )
        )
    return items


class SyncCursor:
    """记录已经写入过 A_Memorix 的条目 ID，避免重复调用。"""

    def __init__(self, path: Path, max_entries: int = 2000) -> None:
        self._path = path
        self._max_entries = max(100, int(max_entries))
        self._synced: Dict[str, str] = {}

    def load(self) -> None:
        """读取游标；文件不存在或损坏时从空游标开始。"""

        if not self._path.exists():
            self._synced = {}
            return
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self._synced = {}
            return
        raw = payload.get("synced") if isinstance(payload, dict) else {}
        self._synced = {str(key): str(value) for key, value in (raw or {}).items()}

    def contains(self, external_id: str) -> bool:
        """判断条目是否已经同步过。"""

        return external_id in self._synced

    def mark(self, external_ids: Iterable[str]) -> None:
        """把条目记为已同步。"""

        now = datetime.now().isoformat(timespec="seconds")
        for external_id in external_ids:
            self._synced[str(external_id)] = now

    def save(self) -> None:
        """按时间保留最近的若干条并落盘。"""

        if len(self._synced) > self._max_entries:
            ordered = sorted(self._synced.items(), key=lambda item: item[1], reverse=True)
            self._synced = dict(ordered[: self._max_entries])
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "synced": self._synced,
        }
        self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
