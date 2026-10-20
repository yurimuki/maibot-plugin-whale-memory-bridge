"""whale → A_Memorix 记忆桥。

把 whale 日记「看过」表格与可选的事件流增量写入 A_Memorix，作用域固定为配置里
的私聊聊天流（默认雪糕私聊），因此群聊检索不到这些记忆。

设计要点：
- **只读 whale 的文件**，不修改 whale 的任何状态；
- **幂等**：每条记忆用稳定的 external_id，重复运行不会重复入库；
- **游标**：已同步的 ID 记在本地 JSON，避免每次全量调用；
- **不阻塞事件循环**：文件读写走 asyncio.to_thread；
- **无 embedding 也能跑**：A_Memorix 会以 sparse/metadata-only 模式落库。
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
    config_version: str = Field(default="1.1.0", description="配置版本")


class BridgeSourceConfig(PluginConfigBase):
    """whale 数据来源。"""

    __ui_label__ = "数据来源"

    whale_home: str = Field(default="~/.dsh", description="whale 的 DSH 家目录")
    diary_file: str = Field(default="whale-diary.md", description="叙事日记文件名")
    private_diary_file: str = Field(default="whale-diary-private.md", description="私密日记文件名")
    include_private_diary: bool = Field(default=True, description="是否把私密日记也写入长期记忆")
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
    digest: BridgeDigestConfig = Field(default_factory=BridgeDigestConfig)


class WhaleMemoryBridgePlugin(MaiBotPlugin):
    """把 whale 的记忆增量同步进 A_Memorix。"""

    config_model = WhaleMemoryBridgeConfig

    def __init__(self) -> None:
        """初始化后台任务状态。"""

        super().__init__()
        self._task: Optional[asyncio.Task[None]] = None
        self._stop = asyncio.Event()
        self._last_run_at: str = ""
        self._last_error: str = ""
        self._last_summary: Dict[str, Any] = {}
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
        cursor = home / self.config.source.cursor_file
        return diary, event_log, cursor

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
        for item in batch:
            if self.config.target.dry_run:
                self.ctx.logger.info(f"[dry-run] {item.external_id} :: {item.render_text()[:80]}")
                written_ids.append(item.external_id)
                continue
            try:
                payload = self._build_payload(item)
                reply = await self.ctx.call_capability("knowledge.ingest", **payload)
            except Exception as exc:  # noqa: BLE001 - 单条失败不应中断整批
                result["failed"] += 1
                self._last_error = f"{item.external_id}: {type(exc).__name__}: {exc}"
                self.ctx.logger.warning(f"whale 记忆写入失败: {item.external_id}: {exc}")
                continue
            if not isinstance(reply, dict) or reply.get("success") is False:
                result["failed"] += 1
                detail = reply.get("error") if isinstance(reply, dict) else reply
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

        cursor.mark(written_ids)
        await asyncio.to_thread(cursor.save)

        result["remaining"] = max(0, len(pending) - len(batch))
        self._last_summary = result
        self._last_run_at = datetime.now().isoformat(timespec="seconds")
        if result["written"] or result["queued"] or result["skipped_exists"] or result["failed"]:
            self.ctx.logger.info(
                "whale 记忆同步: "
                f"扫描 {result['scanned']} / 待写 {result['pending']} / 新写 {result['written']} / "
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

        output_path = self._home() / self.config.digest.output_file
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
        return {
            "success": True,
            "last_run_at": self._last_run_at,
            "last_summary": self._last_summary,
            "last_digest": self._last_digest,
            "last_error": self._last_error,
            "synced_ids": synced,
            "target_session_id": self.config.target.target_session_id,
        }


    @Tool(
        "whale_memory_read",
        description="读取 whale 外部记忆摘要文件（whale-memory.md），绕过 MCP filesystem 的目录限制",
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
        """直接读取摘要文件，避免 agent 用文件工具访问 ~/.dsh 时被拒。"""

        _ = kwargs
        path = self._home() / self.config.digest.output_file
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
        """检索 whale 日记记忆，并按章节/日期/隐私过滤。"""

        target = str(self.config.target.target_session_id or "").strip()
        if not target:
            raise ValueError("target.target_session_id 未配置")
        clean_limit = max(1, min(20, int(limit or 5)))
        query_text = (query or "").strip() or "whale 日记"
        reply = await self.ctx.call_capability(
            "knowledge.search_hits",
            query=query_text,
            chat_id=target,
            limit=max(clean_limit * 4, 20),
            mode="search",
            respect_filter=False,
        )
        if not isinstance(reply, dict):
            return []
        raw_hits = [item for item in (reply.get("hits") or []) if isinstance(item, dict)]

        section_text = (section or "").strip()
        wanted_sections = {section_text} if section_text else set()
        if section_text in SECTION_LABELS:
            wanted_sections.add(SECTION_LABELS[section_text])
        elif section_text:
            label = next((key for key, value in SECTION_LABELS.items() if value == section_text), "")
            if label:
                wanted_sections.add(label)

        results: List[Dict[str, Any]] = []
        for hit in raw_hits:
            if not self._is_whale_hit(hit):
                continue
            meta = self._hit_metadata(hit)
            if wanted_sections and str(meta.get("section") or "") not in wanted_sections:
                continue
            if date and not str(meta.get("observed_at") or "").startswith(date):
                continue
            if not include_private and str(meta.get("visibility") or "") == "private":
                continue
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
            ToolParameterInfo(name="include_private", param_type=ToolParamType.BOOLEAN, description="是否包含私密日记，默认 false（群聊不要开）", required=False),
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
            ToolParameterInfo(name="include_private", param_type=ToolParamType.BOOLEAN, description="是否包含私密日记，默认 false（群聊不要开）", required=False),
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
        if not picked:
            return {"success": False, "error": f"{day} 没有日记"}
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


def create_plugin() -> WhaleMemoryBridgePlugin:
    """构造插件实例。"""

    return WhaleMemoryBridgePlugin()
