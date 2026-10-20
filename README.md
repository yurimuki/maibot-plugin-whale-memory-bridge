# whale 记忆桥（whale-memory-bridge）

把 **DSH（DeepSeek Harness）** 上 **[dsh-bili-whale](https://github.com/Alterssss/dsh-bili-whale)** 插件
每天写下的「看 B 站」日记，增量写入插件自己的**本地索引**，并提供只读检索工具；
宿主支持长期记忆写入时，额外同步进 A_Memorix，同时生成一份摘要供 whale 每轮读取。

一句话：**让麦麦记住「鲸鱼昨晚看了什么、想了什么」**，也让鲸鱼知道自己被记住了。

---

## 一、它配合谁工作

| 角色 | 是什么 | 在本插件里的作用 |
|---|---|---|
| **DSH（DeepSeek Harness）** | DeepSeek 的 agent 运行时 | 提供 `~/.dsh/` 家目录，whale 的日记与事件流都在这里 |
| **dsh-bili-whale** | DSH 的「刷 B 站」插件：每晚挑视频深读打分，写成日记 | 产出 `whale-diary.md` 与 `whale-log.jsonl` |
| **MaiBot** | QQ 机器人 | 承载本插件与工具 |
| **A_Memorix**（可选） | MaiBot 的长期记忆系统 | 宿主暴露 `knowledge.ingest` 时才写入 |

```
dsh-bili-whale ──写──▶ ~/.dsh/whale-diary.md
                         │
                         ├─▶ 本地索引（插件数据目录，始终可用）
                         │        └─▶ whale_memory_search / context 工具
                         ├─▶ A_Memorix（仅当宿主提供 knowledge.ingest）
                         └─▶ ~/.dsh/whale-memory.md（只读摘要，供 whale 读取）
```

> **没有 dsh-bili-whale 就没有数据源。** 本插件不刷 B 站、不联网，只读文件。

---

## 二、前置条件

1. MaiBot ≥ 1.3.0；
2. DSH 侧已安装 `dsh-bili-whale`，且 `~/.dsh/whale-diary.md` 已生成；
3. A_Memorix **可选**：只有宿主注册了 `knowledge.ingest` / `knowledge.search_hits`
   时才会同步进长期记忆，否则自动降级为「本地索引 + 摘要」，工具照常可用。

> 原生 1.3.x 的插件能力注册表里只有 `knowledge.search`，没有写入能力。
> 因此本插件**不声明**这两个能力；检测到缺失时只记一条日志并继续工作。

---

## 三、安装

把仓库放进 `MaiBot/plugins/whale-memory-bridge/`，然后：

```bash
cd MaiBot/plugins/whale-memory-bridge
cp config.example.toml config.toml   # 仓库不提供含真实 session_id 的配置
```

在 WebUI「插件扩展」里启用（或重启 MaiBot）。日志出现这两行即在工作：

```
whale 记忆桥已启动
whale 外部记忆已更新: ~/.dsh/whale-memory.md（N 块）
```

---

## 四、配置（`config.toml`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `[source].whale_home` | `~/.dsh` | DSH 家目录 |
| `[source].diary_file` | `whale-diary.md` | 公开日记 |
| `[source].private_diary_file` | `whale-diary-private.md` | 私密日记，默认不读 |
| `[source].include_private_diary` | `false` | 是否连私密日记一起索引 |
| `[source].cursor_file` | `whale-memory-bridge.cursor.json` | 游标文件名，**必须落在 `whale_home` 内** |
| `[target].target_session_id` | 空 | A_Memorix 写入目标；留空则只写本地索引 |
| `[target].source_type` | `chat_summary` | A_Memorix 的 source_type |
| `[target].interval_seconds` | `300` | 后台扫描间隔 |
| `[index].enabled` | `true` | 是否维护本地索引 |
| `[index].max_entries` | `5000` | 索引保留上限，超出丢弃最旧 |
| `[privacy].allow_private_tools` | `false` | 工具是否可返回私密章节（硬门禁） |
| `[privacy].digest_include_private` | `false` | 摘要是否包含私密章节 |
| `[digest].enabled` | `true` | 是否生成只读摘要 |

### 路径约束

`cursor_file` 与 `digest.output_file` 会被 `resolve()` 后校验必须位于 `whale_home` 内，
写 `../` 越界会直接报错。插件只写这两个文件，不修改 whale 的任何状态。

---

## 五、提供的工具

不注册聊天指令，全部由模型按需调用。

| 工具 | 用途 | 私密内容 |
|---|---|---|
| `whale_seen_recent` | 最近看过的视频 + 一句想法 | 否 |
| `whale_diary_recent` | 某天日记（默认只给「看过」） | 受 `allow_private_tools` 控制 |
| `whale_memory_search` | 检索索引 / 长期记忆 | 受 `allow_private_tools` 控制 |
| `whale_memory_context` | 检索结果整理成可注入文本 | 同上 |
| `whale_memory_read` | 读摘要文件 | 受 `digest_include_private` 控制 |
| `whale_memory_sync` / `_digest` / `_status` | 手动同步 / 生成摘要 / 查看状态 | 管理用 |

### 私密内容怎么界定

即使写在**公开**日记里，下面几类章节也按私密内容处理：

```
letter（给主人的信） / thoughts（想说但还没说） / preference / private_note
```

工具拿不到「当前是群聊还是私聊」，所以不做提示词层的劝退，而是用
`[privacy].allow_private_tools` 做**硬开关**：默认关闭时，上述章节在检索、
上下文、日记工具里一律不返回；`whale_seen_recent`（群聊播报用）永远不含它们。

---

## 六、数据与隐私

- **只读** whale 的源文件，不修改 whale 的任何状态；
- 本地索引写在插件数据目录（`config.index.file`），不进入 MaiBot 聊天库；
- 写入 `whale_home` 的只有游标与摘要两个文件，且路径被限制在 `whale_home` 内；
- 私密章节默认不索引、不写摘要、工具不返回；
- 仓库里的默认配置不含任何个人 session_id 或绝对路径。

---

## 七、已知限制

- 原生 1.3.x 没有 `knowledge.ingest`，因此 A_Memorix 同步会自动跳过，只保留本地索引；
- 日记随日期增长，`whale_memory_digest` 的 prompt 会变长，注意成本；
- 依赖 DSH 与 dsh-bili-whale 的文件格式，上游改格式需同步改 `whale_source.py`；
- 索引是 JSONL 明文，位于插件数据目录，注意备份与访问权限。
