"""whale 日记桥。

把 DSH（DeepSeek Harness）上 dsh-bili-whale 写下的日记，增量写入插件自己的
本地索引，并生成一份只读摘要供 whale 每轮读取；在宿主提供长期记忆写入能力时，
额外把条目同步进 A_Memorix。

设计要点：
- **只读 whale 的源文件**；只在 `whale_home` 下写游标与摘要两个文件；
- **幂等**：每条记忆用稳定的 external_id，重复运行不会重复入库；
- **游标**：已同步的 ID 记在本地 JSON，避免每次全量处理；
- **可降级**：宿主没有 `knowledge.ingest` 时，索引与工具照常工作，
  只有 A_Memorix 同步被跳过。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

from .whale_source import (
    SECTION_LABELS,
    SyncCursor,
    WhaleMemoryItem,
    expand_path,
    parse_diary,
    parse_event_log,
    parse_private_diary,
)


class BridgePluginConfig(PluginConfigBase):
    """插件总开关。"""

    __ui_label__ = "插件"

    enabled: bool = Field(default=True, description="是否启用 whale 记忆桥")
    config_version: str = Field(default="1.3.0", description="配置版本")


class BridgeSourceConfig(PluginConfigBase):
    """whale 数据来源。"""

    __ui_label__ = "数据来源"

    whale_home: str = Field(default="~/.dsh", description="whale 的 DSH 家目录")
    diary_file: str = Field(default="whale-diary.md", description="叙事日记文件名")
    private_diary_file: str = Field(default="whale-diary-private.md", description="私密日记文件名")
    include_private_diary: bool = Field(
        default=False, description="是否把私密日记也写入索引与长期记忆"
    )
    log_file: str = Field(default="whale-log.jsonl", description="事件流文件名")
    cursor_file: str = Field(default="whale-memory-bridge.cursor.json", description="同步游标文件名")
    log_event_kinds: str = Field(default="", description="要从事件流同步的 kind，逗号分隔；留空表示只同步日记")


class BridgeTargetConfig(PluginConfigBase):
    """写入目标。"""

    __ui_label__ = "写入目标"

    target_session_id: str = Field(
        default="",
        description="A_Memorix 的 chat_id（必须是真实聊天流的 session_id）",
    )
    source_type: str = Field(default="chat_summary", description="写入 A_Memorix 的 source_type")
    interval_seconds: int = Field(default=300, description="后台同步间隔（秒）")
    max_items_per_run: int = Field(default=20, description="每轮最多写入多少条")
    dry_run: bool = Field(default=False, description="只解析与记录，不真正写入 A_Memorix")


class BridgeIndexConfig(PluginConfigBase):
    """本地索引：不依赖宿主长期记忆能力也能检索。"""

    __ui_label__ = "本地索引"
    __ui_order__ = 4

    enabled: bool = Field(default=True, description="是否维护本地索引文件")
    file: str = Field(default="whale-index.jsonl", description="索引文件名（落在插件数据目录）")
    max_entries: int = Field(default=5000, description="索引最多保留多少条，超出丢弃最旧的")


class BridgePrivacyConfig(PluginConfigBase):
    """私密内容的硬门禁。"""

    __ui_label__ = "隐私"
    __ui_order__ = 6

    allow_private_tools: bool = Field(
        default=False,
        description="是否允许工具返回私密内容（给主人的信/心声）。工具拿不到聊天流类型，默认关闭即硬门禁",
    )
    digest_include_private: bool = Field(
        default=False, description="反向摘要是否包含私密章节"
    )


class BridgeGroupConfig(PluginConfigBase):
    """群聊播报配置。"""

    __ui_label__ = "群聊"
    __ui_order__ = 5

    max_items: int = Field(default=5, description="群聊里最多播报几条「看过」")


class BridgeDigestConfig(PluginConfigBase):
    """反向摘要：把 A_Memorix 的记忆压成 whale 可读的只读文件。"""

    __ui_label__ = "反向摘要"

    enabled: bool = Field(default=True, description="是否生成 whale 外部记忆摘要")
    output_file: str = Field(default="whale-memory.md", description="摘要文件名（落在 whale_home 下）")
    lookback_days: int = Field(default=7, description="回看天数")
    limit: int = Field(default=12, description="摘要最多条目数")
    max_chars: int = Field(default=4000, description="摘要最大字符数")


class WhaleMemoryBridgeConfig(PluginConfigBase):
    """插件配置。"""

    plugin: BridgePluginConfig = Field(default_factory=BridgePluginConfig)
    source: BridgeSourceConfig = Field(default_factory=BridgeSourceConfig)
    target: BridgeTargetConfig = Field(default_factory=BridgeTargetConfig)
    group: BridgeGroupConfig = Field(default_factory=BridgeGroupConfig)
    index: BridgeIndexConfig = Field(default_factory=BridgeIndexConfig)
    privacy: BridgePrivacyConfig = Field(default_factory=BridgePrivacyConfig)
    digest: BridgeDigestConfig = Field(default_factory=BridgeDigestConfig)


class WhaleMemoryBridgePlugin(MaiBotPlugin):
    """把 whale 的记忆增量同步进 A_Memorix。"""

    config_model = WhaleMemoryBridgeConfig

    # 这些章节即使写在公开日记里，也按私密内容处理
    _PRIVATE_SECTIONS = {"letter", "thoughts", "preference", "private_note"}

    def __init__(self) -> None:
        """初始化后台任务状态。"""

        super().__init__()
        self._task: Optional[asyncio.Task[None]] = None
        self._stop = asyncio.Event()
        self._last_run_at: str = ""
        self._last_error: str = ""
        self._last_summary: Dict[str, Any] = {}
        self._ingest_ok: Optional[bool] = None
        self._hits_ok: Optional[bool] = None
        self._last_digest: Dict[str, Any] = {}

    async def on_load(self) -> None:
        """按配置启动后台同步循环。"""

        if not self.config.plugin.enabled:
            self.ctx.logger.info("whale 记忆桥已禁用（plugin.enabled=false）")
            return
        self._start_loop()
        self.ctx.logger.info("whale 记忆桥已启动")

    async def on_unload(self) -> None:
        """停止后台循环。"""

        await self._stop_loop()

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        """配置变化后重启循环，让新参数立即生效。"""

        _ = (config_data, version)
        if scope != "self":
            return
        await self._stop_loop()
        if self.config.plugin.enabled:
            self._start_loop()

    def _start_loop(self) -> None:
        """创建后台任务。"""

        if self._task is not None and not self._task.done():
            return
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="whale-memory-bridge.sync")

    async def _stop_loop(self) -> None:
        """取消后台任务并等待退出。"""

        self._stop.set()
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        """定时同步；任何异常都记录后继续下一轮。"""

        while not self._stop.is_set():
            try:
                await self._sync_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 后台循环必须活下来
                self._last_error = f"{type(exc).__name__}: {exc}"
                self.ctx.logger.exception("whale 记忆同步失败")
            try:
                await self._sync_digest()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 反向摘要失败不影响正向同步
                self._last_error = f"digest: {type(exc).__name__}: {exc}"
                self.ctx.logger.warning(f"whale 外部记忆摘要生成失败: {exc}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=max(15, int(self.config.target.interval_seconds)))
            except asyncio.TimeoutError:
                continue

    def _home(self) -> Path:
        """whale 的 DSH 家目录。"""

        return expand_path(self.config.source.whale_home)

    def _paths(self) -> tuple[Path, Path, Path]:
        """解析数据来源的三个路径。"""

        home = self._home()
        diary = home / self.config.source.diary_file
        event_log = home / self.config.source.log_file
        cursor = self._safe_under(home, self.config.source.cursor_file)
        return diary, event_log, cursor

    @staticmethod
    def _safe_under(base: Path, name: str) -> Path:
        """把配置里的相对文件名限制在 base 目录内，防止 ../ 越界。"""

        candidate = (base / str(name or "")).resolve()
        root = base.resolve()
        if candidate != root and root not in candidate.parents:
            raise ValueError(f"路径越界：{name!r} 不在 {root} 内")
        return candidate

    def _index_path(self) -> Path:
        """本地索引文件路径（插件数据目录内）。"""

        return self._safe_under(self.ctx.paths.data_dir, self.config.index.file or "whale-index.jsonl")

    async def _index_load(self) -> List[Dict[str, Any]]:
        """读取本地索引。"""

        path = self._index_path()
        if not path.exists():
            return []

        def _read() -> List[Dict[str, Any]]:
            entries: List[Dict[str, Any]] = []
            try:
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(data, dict):
                        entries.append(data)
            except OSError as exc:
                self.ctx.logger.warning(f"读取本地索引失败: {exc}")
            return entries

        return await asyncio.to_thread(_read)

    async def _index_append(self, items: List[WhaleMemoryItem]) -> int:
        """把条目追加进本地索引（按 external_id 去重）。"""

        if not self.config.index.enabled or not items:
            return 0
        path = self._index_path()
        known = {str(entry.get("external_id") or "") for entry in await self._index_load()}
        fresh = [item for item in items if item.external_id not in known]
        if not fresh:
            return 0
        limit = max(100, int(self.config.index.max_entries))

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                for item in fresh:
                    handle.write(
                        json.dumps(
                            {
                                "external_id": item.external_id,
                                "observed_at": item.observed_at,
                                "section": item.section,
                                "entry_label": item.entry_label,
                                "bv": item.bv,
                                "title": item.title,
                                "thought": item.thought,
                                "text": item.render_text(),
                                "visibility": item.visibility,
                                "sensitive": item.section in self._PRIVATE_SECTIONS
                                or item.visibility == "private",
                                "score": item.score,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
            kept = path.read_text(encoding="utf-8").splitlines()[-limit:]
            path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")

        await asyncio.to_thread(_write)
        return len(fresh)

    async def _search_index(
        self, query: str, limit: int, section: str = "", date: str = "", include_private: bool = False
    ) -> List[Dict[str, Any]]:
        """在本地索引里做关键词检索。"""

        entries = await self._index_load()
        needle = (query or "").strip().lower()
        wanted_section = (section or "").strip()
        results: List[Dict[str, Any]] = []
        for entry in reversed(entries):
            sensitive = bool(entry.get("sensitive")) or str(entry.get("visibility") or "") == "private"
            if not include_private and sensitive:
                continue
            if wanted_section:
                raw_section = str(entry.get("section") or "")
                label = SECTION_LABELS.get(raw_section, raw_section)
                if wanted_section not in (raw_section, label):
                    continue
            if date and not str(entry.get("observed_at") or "").startswith(date):
                continue
            text = str(entry.get("text") or "")
            if needle and needle not in text.lower():
                continue
            results.append(
                {
                    "content": text,
                    "score": entry.get("score"),
                    "source": "index",
                    "hash": None,
                    "title": entry.get("title"),
                    "metadata": {
                        "bridge": "whale-memory-bridge",
                        "section": entry.get("section"),
                        "entry_label": entry.get("entry_label"),
                        "bv": entry.get("bv"),
                        "title": entry.get("title"),
                        "visibility": entry.get("visibility"),
                        "observed_at": entry.get("observed_at"),
                        "score": entry.get("score"),
                    },
                }
            )
            if len(results) >= limit:
                break
        return results

    def _private_allowed(self) -> bool:
        """私密内容是否被配置放行。"""

        return bool(self.config.privacy.allow_private_tools)

    def _private_diary_path(self) -> Path:
        """私密日记路径。"""

        return self._home() / self.config.source.private_diary_file

    def _event_kinds(self) -> Set[str]:
        """解析事件流 kind 白名单。"""

        raw = str(self.config.source.log_event_kinds or "")
        return {item.strip() for item in raw.split(",") if item.strip()}

    async def _collect_items(self) -> List[WhaleMemoryItem]:
        """从日记与事件流收集条目并按日期排序。"""

        diary, event_log, _ = self._paths()
        items = await asyncio.to_thread(parse_diary, diary, visibility="chat")
        if self.config.source.include_private_diary:
            private_diary = self._private_diary_path()
            items.extend(await asyncio.to_thread(parse_private_diary, private_diary))
        kinds = self._event_kinds()
        if kinds:
            items.extend(await asyncio.to_thread(parse_event_log, event_log, kinds))
        items.sort(key=lambda item: (item.observed_at, item.external_id))
        return items

    async def _sync_once(self, force: bool = False) -> Dict[str, Any]:
        """执行一轮增量同步；force=True 时忽略游标全量重试。"""

        diary, _, cursor_path = self._paths()
        if not diary.exists():
            self._last_summary = {"skipped": "diary_missing", "diary": str(diary)}
            return dict(self._last_summary)

        cursor = SyncCursor(cursor_path)
        await asyncio.to_thread(cursor.load)

        items = await self._collect_items()
        pending = items if force else [item for item in items if not cursor.contains(item.external_id)]
        limit = max(1, int(self.config.target.max_items_per_run))
        batch = pending[:limit]

        result: Dict[str, Any] = {
            "scanned": len(items),
            "pending": len(pending),
            "written": 0,
            "skipped_exists": 0,
            "queued": 0,
            "failed": 0,
            "dry_run": bool(self.config.target.dry_run),
        }
        written_ids: List[str] = []
        indexed = 0
        memory_skipped = False
        if not self.config.target.dry_run:
            indexed = await self._index_append(batch)

        for item in batch:
            if self.config.target.dry_run:
                self.ctx.logger.info(f"[dry-run] {item.external_id} :: {item.render_text()[:80]}")
                written_ids.append(item.external_id)
                continue
            # 本地索引已覆盖该条；长期记忆不可用时只跳过一次写入
            if not self.config.target.target_session_id or self._ingest_ok is False:
                memory_skipped = True
                written_ids.append(item.external_id)
                continue
            try:
                payload = self._build_payload(item)
                reply = await self.ctx.call_capability("knowledge.ingest", **payload)
            except Exception as exc:  # noqa: BLE001 - 单条失败不应中断整批
                if self._ingest_ok is not False and _capability_missing(exc):
                    self._ingest_ok = False
                    memory_skipped = True
                    self.ctx.logger.info(
                        "宿主未提供 knowledge.ingest，后续只写本地索引（工具仍可用）"
                    )
                    written_ids.append(item.external_id)
                    continue
                result["failed"] += 1
                self._last_error = f"{item.external_id}: {type(exc).__name__}: {exc}"
                self.ctx.logger.warning(f"whale 记忆写入失败: {item.external_id}: {exc}")
                continue
            if not isinstance(reply, dict) or reply.get("success") is False:
                detail = reply.get("error") if isinstance(reply, dict) else reply
                if self._ingest_ok is not False and _capability_missing(detail):
                    self._ingest_ok = False
                    memory_skipped = True
                    self.ctx.logger.info(
                        "宿主未提供 knowledge.ingest，后续只写本地索引（工具仍可用）"
                    )
                    written_ids.append(item.external_id)
                    continue
                result["failed"] += 1
                self._last_error = f"{item.external_id}: {detail}"
                self.ctx.logger.warning(f"whale 记忆写入被拒绝: {item.external_id}: {detail}")
                continue
            if reply.get("queued"):
                result["queued"] += 1
            elif reply.get("stored_ids"):
                result["written"] += 1
            else:
                result["skipped_exists"] += 1
            written_ids.append(item.external_id)

        result["indexed"] = indexed
        result["memory_skipped"] = memory_skipped
        cursor.mark(written_ids)
        await asyncio.to_thread(cursor.save)

        result["remaining"] = max(0, len(pending) - len(batch))
        self._last_summary = result
        self._last_run_at = datetime.now().isoformat(timespec="seconds")
        if result["written"] or result["queued"] or result["skipped_exists"] or result["failed"]:
            self.ctx.logger.info(
                "whale 记忆同步: "
                f"扫描 {result['scanned']} / 待写 {result['pending']} / 索引新增 {result.get('indexed', 0)} / "
                f"新写 {result['written']} / "
                f"已存在 {result['skipped_exists']} / 排队 {result['queued']} / 失败 {result['failed']}"
            )
        return result

    async def _sync_digest(self) -> Dict[str, Any]:
        """从日记原文生成 whale 可读的只读摘要，不再走通用聊天记忆检索。"""

        if not self.config.digest.enabled:
            self._last_digest = {"skipped": "disabled"}
            return dict(self._last_digest)

        lookback_days = max(1, int(self.config.digest.lookback_days))
        limit = max(1, int(self.config.digest.limit))
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        all_items = await self._collect_items()
        if not self.config.privacy.digest_include_private:
            all_items = [
                item
                for item in all_items
                if item.visibility != "private" and item.section not in self._PRIVATE_SECTIONS
            ]
        items = [item for item in all_items if (item.observed_at or "") >= cutoff]
        if not items:
            items = all_items[-limit:]

        groups: Dict[Tuple[str, str], List[WhaleMemoryItem]] = {}
        for item in items:
            groups.setdefault((item.observed_at, item.entry_label), []).append(item)

        priority = {"letter": 0, "thoughts": 1, "preference": 2, "private_note": 3, "seen": 4, "event": 5}
        blocks: List[str] = []
        block_count = 0
        for (date, label), group in sorted(
            groups.items(), key=lambda pair: (pair[0][0], _entry_time(pair[0][1])), reverse=True
        ):
            heading = " · ".join(part for part in (date, label) if part)
            section_lines: List[str] = [f"### {heading or '日期未知'}"]
            prose = [item for item in group if item.section != "seen"]
            seen = [item for item in group if item.section == "seen"]
            for item in sorted(prose, key=lambda entry: priority.get(entry.section, 9)):
                section_name = SECTION_LABELS.get(item.section, item.section)
                if item.visibility == "private":
                    section_name += "（私密）"
                body = (item.raw_text or item.thought).strip()
                if len(body) > 700:
                    body = body[:700].rstrip() + "…"
                section_lines.append(f"**{section_name}**\n{body}")
                block_count += 1
            if seen:
                bullets = []
                for item in seen:
                    title = item.title or "（无标题）"
                    thought = (item.thought or "").strip()
                    if len(thought) > 160:
                        thought = thought[:160].rstrip() + "…"
                    prefix = f"{item.bv}｜" if item.bv else ""
                    bullets.append(f"- {prefix}{title}｜{thought}")
                section_lines.append("**看过**\n" + "\n".join(bullets))
                block_count += 1
            blocks.extend(section_lines)
            if block_count >= limit:
                break

        generated = datetime.now().strftime("%Y-%m-%d %H:%M")
        header = [
            "# whale 外部记忆（只读）",
            "",
            f"> 由 whale-memory-bridge 从 whale 日记原文生成，最后更新：{generated}。",
            "> 只读参考：不要写回本文件，也不要逐字复述其中的隐私内容。",
            "",
            f"## 最近 {lookback_days} 天",
        ]
        text = "\n".join(header + (blocks or ["（暂无）"])) + "\n"
        max_chars = max(500, int(self.config.digest.max_chars))
        truncated = len(text) > max_chars
        if truncated:
            text = text[:max_chars].rstrip() + "\n\n（已截断）\n"

        output_path = self._safe_under(self._home(), self.config.digest.output_file)
        changed = await asyncio.to_thread(_write_atomic, output_path, text)
        self._last_digest = {
            "path": str(output_path),
            "hits": block_count,
            "changed": changed,
            "generated_at": generated,
            "truncated": truncated,
            "source": "diary",
        }
        if changed:
            self.ctx.logger.info(f"whale 外部记忆已更新: {output_path}（{block_count} 块）")
        return dict(self._last_digest)

    def _build_payload(self, item: WhaleMemoryItem) -> Dict[str, Any]:
        """把条目转成 A_Memorix 的 ingest_text 参数。"""

        tags = ["whale", "diary", item.section, item.visibility]
        if item.bv:
            tags.append(item.bv)
        return {
            "external_id": item.external_id,
            "source_type": self.config.target.source_type,
            "text": item.render_text(),
            "chat_id": self.config.target.target_session_id,
            "timestamp": _parse_timestamp(item.observed_at),
            "tags": tags,
            "metadata": {
                "bridge": "whale-memory-bridge",
                "section": item.section,
                "entry_label": item.entry_label,
                "bv": item.bv,
                "title": item.title,
                "score": item.score,
                "visibility": item.visibility,
                "observed_at": item.observed_at,
            },
            "respect_filter": False,
        }

    @Tool(
        "whale_memory_sync",
        description="立即同步一次 whale 日记到长期记忆",
        parameters=[
            ToolParameterInfo(
                name="force",
                param_type=ToolParamType.BOOLEAN,
                description="忽略游标全量重试（写入本身幂等）",
                required=False,
            )
        ],
    )
    async def handle_sync(self, force: bool = False, **kwargs: Any) -> Dict[str, Any]:
        """手动触发一轮同步。"""

        _ = kwargs
        summary = await self._sync_once(force=bool(force))
        return {"success": True, "summary": summary, "last_error": self._last_error}

    @Tool("whale_memory_digest", description="立即从长期记忆生成 whale 外部记忆摘要文件")
    async def handle_digest(self, **kwargs: Any) -> Dict[str, Any]:
        """手动刷新反向摘要。"""

        _ = kwargs
        digest = await self._sync_digest()
        return {"success": True, "digest": digest, "last_error": self._last_error}

    @Tool("whale_memory_status", description="查看 whale 记忆桥的同步状态")
    async def handle_status(self, **kwargs: Any) -> Dict[str, Any]:
        """返回最近一次同步结果与游标统计。"""

        _ = kwargs
        _, _, cursor_path = self._paths()
        cursor = SyncCursor(cursor_path)
        await asyncio.to_thread(cursor.load)
        synced = 0
        if cursor_path.exists():
            try:
                payload = json.loads(cursor_path.read_text(encoding="utf-8"))
                synced = len(payload.get("synced") or {})
            except (OSError, json.JSONDecodeError):
                synced = 0
        index_entries = len(await self._index_load()) if self.config.index.enabled else 0
        return {
            "success": True,
            "last_run_at": self._last_run_at,
            "last_summary": self._last_summary,
            "last_digest": self._last_digest,
            "last_error": self._last_error,
            "synced_ids": synced,
            "index_entries": index_entries,
            "index_enabled": bool(self.config.index.enabled),
            "long_term_memory": {
                "target_session_id": self.config.target.target_session_id,
                "ingest_available": self._ingest_ok is not False,
                "search_hits_available": self._hits_ok is not False,
            },
            "private_tools_allowed": self._private_allowed(),
        }


    @Tool(
        "whale_memory_read",
        description="直接返回 whale 外部记忆摘要（whale-memory.md）的内容，省得 agent 再走文件工具",
        parameters=[
            ToolParameterInfo(
                name="max_chars",
                param_type=ToolParamType.INTEGER,
                description="最多返回多少个字符；0 表示完整返回",
                required=False,
            )
        ],
    )
    async def handle_read(self, max_chars: int = 0, **kwargs: Any) -> Dict[str, Any]:
        """返回摘要文件内容。"""

        _ = kwargs
        path = self._safe_under(self._home(), self.config.digest.output_file)
        if not path.exists():
            return {"success": False, "path": str(path), "error": "摘要文件不存在"}
        try:
            content = await asyncio.to_thread(path.read_text, encoding="utf-8")
            file_stat = await asyncio.to_thread(path.stat)
        except OSError as exc:
            return {"success": False, "path": str(path), "error": f"{type(exc).__name__}: {exc}"}
        total_chars = len(content)
        limit = max(0, int(max_chars or 0))
        truncated = limit > 0 and total_chars > limit
        if truncated:
            content = content[:limit] + "\n\n…（已截断）"
        return {
            "success": True,
            "path": str(path),
            "size": file_stat.st_size,
            "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(timespec="seconds"),
            "total_chars": total_chars,
            "truncated": truncated,
            "content": content,
        }


    @staticmethod
    def _hit_metadata(hit: Dict[str, Any]) -> Dict[str, Any]:
        """读取检索结果的 metadata，兼容 JSON 字符串。"""

        raw = hit.get("metadata")
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @classmethod
    def _is_whale_hit(cls, hit: Dict[str, Any]) -> bool:
        """只保留 whale 日记/视频记忆，排除普通聊天摘要。"""

        meta = cls._hit_metadata(hit)
        tags = meta.get("tags") or []
        if isinstance(tags, str):
            tags = [tags]
        return (
            str(meta.get("bridge") or "") == "whale-memory-bridge"
            or "whale" in tags
        )

    async def _search_whale(
        self,
        query: str,
        limit: int,
        section: str = "",
        date: str = "",
        include_private: bool = False,
    ) -> List[Dict[str, Any]]:
        """检索 whale 记忆：本地索引优先，长期记忆可用时再补充。"""

        clean_limit = max(1, min(20, int(limit or 5)))
        # 工具拿不到聊天流类型，私密内容只能靠配置做硬门禁
        include_private = include_private and self._private_allowed()
        results = await self._search_index(query, clean_limit, section, date, include_private)

        target = str(self.config.target.target_session_id or "").strip()
        if len(results) >= clean_limit or not target or self._hits_ok is False:
            return results

        query_text = (query or "").strip() or "whale 日记"
        try:
            reply = await self.ctx.call_capability(
                "knowledge.search_hits",
                query=query_text,
                chat_id=target,
                limit=max(clean_limit * 4, 20),
                mode="search",
                respect_filter=False,
            )
        except Exception as exc:  # noqa: BLE001 - 能力不可用时回退到本地索引
            if _capability_missing(exc):
                self._hits_ok = False
                self.ctx.logger.info("宿主未提供 knowledge.search_hits，检索只用本地索引")
            return results
        if not isinstance(reply, dict) or reply.get("success") is False:
            detail = reply.get("error") if isinstance(reply, dict) else reply
            if _capability_missing(detail):
                self._hits_ok = False
                self.ctx.logger.info("宿主未提供 knowledge.search_hits，检索只用本地索引")
            return results

        section_text = (section or "").strip()
        wanted_sections = {section_text} if section_text else set()
        if section_text in SECTION_LABELS:
            wanted_sections.add(SECTION_LABELS[section_text])
        elif section_text:
            label = next((key for key, value in SECTION_LABELS.items() if value == section_text), "")
            if label:
                wanted_sections.add(label)

        seen = {str(item.get("hash") or item.get("content") or "") for item in results}
        for hit in [item for item in (reply.get("hits") or []) if isinstance(item, dict)]:
            if not self._is_whale_hit(hit):
                continue
            meta = self._hit_metadata(hit)
            if wanted_sections and str(meta.get("section") or "") not in wanted_sections:
                continue
            if date and not str(meta.get("observed_at") or "").startswith(date):
                continue
            if not include_private and (
                str(meta.get("visibility") or "") == "private"
                or str(meta.get("section") or "") in self._PRIVATE_SECTIONS
            ):
                continue
            signature = str(hit.get("hash") or hit.get("content") or "")
            if signature in seen:
                continue
            seen.add(signature)
            results.append(
                {
                    "content": str(hit.get("content") or ""),
                    "score": hit.get("score"),
                    "source": hit.get("source"),
                    "hash": hit.get("hash"),
                    "title": hit.get("title"),
                    "metadata": meta,
                }
            )
            if len(results) >= clean_limit:
                break
        return results

    @Tool(
        "whale_memory_search",
        description="检索 whale 日记记忆（给主人的信/看过/想说但还没说/私密）",
        parameters=[
            ToolParameterInfo(name="query", param_type=ToolParamType.STRING, description="检索词；留空则查 whale 日记", required=False),
            ToolParameterInfo(name="limit", param_type=ToolParamType.INTEGER, description="返回条数，默认 5", required=False),
            ToolParameterInfo(name="section", param_type=ToolParamType.STRING, description="章节过滤：letter/seen/thoughts/preference/private_note", required=False),
            ToolParameterInfo(name="date", param_type=ToolParamType.STRING, description="日期过滤，YYYY-MM-DD", required=False),
            ToolParameterInfo(name="include_private", param_type=ToolParamType.BOOLEAN, description="是否包含私密日记；只有 privacy.allow_private_tools 打开时才生效", required=False),
        ],
    )
    async def handle_search(
        self,
        query: str = "",
        limit: int = 5,
        section: str = "",
        date: str = "",
        include_private: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """返回结构化检索结果。"""

        _ = kwargs
        hits = await self._search_whale(query, limit, section, date, bool(include_private))
        return {"success": True, "query": query, "count": len(hits), "hits": hits}

    @Tool(
        "whale_memory_context",
        description="把 whale 日记检索结果整理成可直接放进上下文的文本",
        parameters=[
            ToolParameterInfo(name="query", param_type=ToolParamType.STRING, description="检索词", required=False),
            ToolParameterInfo(name="limit", param_type=ToolParamType.INTEGER, description="最多几条，默认 5", required=False),
            ToolParameterInfo(name="section", param_type=ToolParamType.STRING, description="章节过滤", required=False),
            ToolParameterInfo(name="date", param_type=ToolParamType.STRING, description="日期过滤，YYYY-MM-DD", required=False),
            ToolParameterInfo(name="max_chars", param_type=ToolParamType.INTEGER, description="上下文最大字符数，默认 2000", required=False),
            ToolParameterInfo(name="include_private", param_type=ToolParamType.BOOLEAN, description="是否包含私密日记；只有 privacy.allow_private_tools 打开时才生效", required=False),
        ],
    )
    async def handle_context(
        self,
        query: str = "",
        limit: int = 5,
        section: str = "",
        date: str = "",
        max_chars: int = 2000,
        include_private: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """返回适合注入提示词的 whale 日记上下文。"""

        _ = kwargs
        hits = await self._search_whale(query, limit, section, date, bool(include_private))
        lines: List[str] = []
        for hit in hits:
            meta = hit.get("metadata") or {}
            parts = [str(meta.get("observed_at") or ""), str(meta.get("entry_label") or "")]
            section_label = SECTION_LABELS.get(str(meta.get("section") or ""), str(meta.get("section") or ""))
            if section_label:
                parts.append(section_label)
            if str(meta.get("visibility") or "") == "private":
                parts.append("私密")
            label = " · ".join(part for part in parts if part)
            content = " ".join(str(hit.get("content") or "").split())
            if content:
                lines.append(f"[{label or 'whale 日记'}] {content}")
        context = "\n\n".join(lines) or "（没有找到相关 whale 日记）"
        limit_chars = max(200, min(8000, int(max_chars or 2000)))
        truncated = len(context) > limit_chars
        if truncated:
            context = context[:limit_chars].rstrip() + "\n\n…（已截断）"
        return {
            "success": True,
            "query": query,
            "count": len(hits),
            "truncated": truncated,
            "context": context,
        }


    # ------------------------------------------------------------------
    # 公开播报工具：群友 @ 问「看过什么」时由模型调用
    # 日记原文是写给「主人」的私人叙事，这里只暴露「看过」，
    # 并清洗成适合群聊的一句想法。
    # ------------------------------------------------------------------

    _PLACEHOLDER_TITLES = {"", "—", "-", "今晚没看", "没看", "无"}

    async def _diary_items(self) -> List[WhaleMemoryItem]:
        """读取公开日记条目（不含私密日记文件）。"""

        diary, _, _ = self._paths()
        return await asyncio.to_thread(parse_diary, diary, visibility="chat")

    @staticmethod
    def _clean_fragment(text: str, limit: int = 60) -> str:
        """把日记里的书面/亲昵口吻压成一句随口的话。"""

        raw = " ".join((text or "").split())
        raw = raw.replace("主人", "").replace("亲爱的", "")
        raw = re.sub(r"^(深读|略读|只读字|只看字|快速过|速览)[。.：:\s]*", "", raw)
        raw = raw.strip(" 　")
        return raw[:limit].strip()

    @classmethod
    def _real_seen(cls, items: List[WhaleMemoryItem]) -> List[WhaleMemoryItem]:
        """剔除「今晚没看」这类占位行。"""

        picked: List[WhaleMemoryItem] = []
        for item in items:
            title = (item.title or "").strip()
            bv = (item.bv or "").strip()
            if title in cls._PLACEHOLDER_TITLES:
                continue
            if bv in cls._PLACEHOLDER_TITLES and title in cls._PLACEHOLDER_TITLES:
                continue
            picked.append(item)
        return picked

    @Tool(
        "whale_seen_recent",
        description=(
            "群友问「最近看了什么 / 刷到什么 / 有什么好玩的」时调用。"
            "返回最近看过的视频标题和她的一句想法（已剔除写给主人的信与私密内容），"
            "你再用自己的话简短说出来，不要照抄成清单。"
        ),
        parameters=[
            ToolParameterInfo(
                name="limit",
                param_type=ToolParamType.INTEGER,
                description="最多几条，默认按插件配置（通常 5）",
                required=False,
            )
        ],
    )
    async def handle_seen_recent(self, limit: int = 0, **kwargs: Any) -> Dict[str, Any]:
        """返回可公开的「最近看过」结构化数据。"""

        _ = kwargs
        items = await self._diary_items()
        seen = self._real_seen([item for item in items if item.section == "seen"])
        seen.sort(key=lambda item: (item.observed_at, item.external_id), reverse=True)
        default_limit = max(1, min(10, int(self.config.group.max_items)))
        count = max(1, min(10, int(limit or default_limit)))
        videos = [
            {
                "date": item.observed_at,
                "title": self._clean_fragment(item.title, 40),
                "thought": self._clean_fragment(item.thought, 50),
            }
            for item in seen[:count]
        ]
        return {"success": True, "count": len(videos), "videos": videos}

    @Tool(
        "whale_diary_recent",
        description=(
            "读取 whale 最近一次日记的完整内容，包含「给主人的信」与「想说但还没说」。"
            "这些是私人内容，只在私聊、或确认对方是主人时使用；群里不要用。"
        ),
        parameters=[
            ToolParameterInfo(
                name="date",
                param_type=ToolParamType.STRING,
                description="日期 YYYY-MM-DD，留空取最近一天",
                required=False,
            )
        ],
    )
    async def handle_diary_recent(self, date: str = "", **kwargs: Any) -> Dict[str, Any]:
        """返回最近一天（或指定日期）的完整日记片段。"""

        _ = kwargs
        items = await self._diary_items()
        if not items:
            return {"success": False, "error": "还没有日记"}
        day = (date or "").strip() or max(item.observed_at for item in items)
        picked = [item for item in items if item.observed_at == day]
        if not self._private_allowed():
            picked = [item for item in picked if item.section not in {"letter", "thoughts", "preference", "private_note"}]
        if not picked:
            return {"success": False, "error": f"{day} 没有可公开的日记（私密章节已在 privacy 配置里关闭）"}
        payload = [
            {
                "section": SECTION_LABELS.get(item.section, item.section),
                "label": item.entry_label,
                "title": item.title,
                "bv": item.bv,
                "thought": item.thought,
                "body": (item.raw_text or "").strip(),
            }
            for item in picked
        ]
        return {"success": True, "date": day, "entries": payload}

def _write_atomic(path: Path, text: str) -> bool:
    """内容变化时原子写入；返回是否真的写了。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == text:
                return False
        except OSError:
            pass
    temp_path = path.with_name(path.name + ".tmp")
    temp_path.write_text(text, encoding="utf-8")
    os.replace(temp_path, path)
    return True


def _entry_time(label: str) -> int:
    """从日记标题里提取 HH:MM，用于同一天多趟时排序。"""

    match = re.search(r"(\d{1,2}):(\d{2})", label or "")
    if not match:
        return 0
    return int(match.group(1)) * 60 + int(match.group(2))


def _parse_timestamp(observed_at: str) -> Optional[float]:
    """把 YYYY-MM-DD 转成时间戳；解析不了就返回 None。"""

    if not observed_at:
        return None
    try:
        return datetime.strptime(observed_at, "%Y-%m-%d").timestamp()
    except ValueError:
        return None


def _capability_missing(detail: Any) -> bool:
    """判断宿主是否缺少某个能力（未注册或未授权）。"""

    text = str(detail or "").lower()
    return any(
        marker in text
        for marker in (
            "未注册的能力",
            "未授权",
            "未获授权",
            "not registered",
            "unauthorized",
            "permission denied",
        )
    )


def create_plugin() -> WhaleMemoryBridgePlugin:
    """构造插件实例。"""

    return WhaleMemoryBridgePlugin()
