# whale 记忆桥（whale-memory-bridge）

把 **DSH（DeepSeek Harness）** 上 **[dsh-bili-whale](https://github.com/Alterssss/dsh-bili-whale)** 插件
每天写下的「看 B 站」日记，增量同步进 MaiBot 的 **A_Memorix 长期记忆**，
并反向生成一份摘要供 whale 每轮醒来读取。

一句话：**让麦麦记住「鲸鱼昨晚看了什么、想了什么」**，也让鲸鱼知道自己被记住了。

---

## 一、它配合谁工作

| 角色 | 是什么 | 在本插件里的作用 |
|---|---|---|
| **DSH（DeepSeek Harness）** | DeepSeek 的 agent 运行时 | 提供 `~/.dsh/` 家目录，whale 的日记、事件流、游标都在这里 |
| **dsh-bili-whale** | DSH 的「刷 B 站」插件：用一个小号登录、每晚挑视频深读打分，写成日记 | 产出 `whale-diary.md`（给主人的信 / 看过 / 想说但还没说）与 `whale-log.jsonl` |
| **MaiBot + A_Memorix** | QQ 机器人与其长期记忆系统 | 接收本插件写入的记忆，供麦麦在聊天时检索 |

> **没有 dsh-bili-whale 就没有数据源。** 本插件不刷 B 站、不联网，只读文件。

```
dsh-bili-whale ──写──▶ ~/.dsh/whale-diary.md
                         │
                         │ ① 正向同步（增量、幂等、带游标）
                         ▼
                    A_Memorix（指定聊天流作用域）
                         │
                         │ ② 反向摘要
                         ▼
              ~/.dsh/whale-memory.md ──读──▶ dsh-bili-whale 每轮醒来
```

---

## 二、前置条件

1. MaiBot ≥ 1.3.0，且 **A_Memorix 已启用**；
2. A_Memorix 配置了 **embedding 模型**（未配置时只有关键词召回，写入会降级）；
3. 有一个确定的「目标聊天流」`session_id`（记忆写进这个聊天流的检索作用域）；
4. DSH 侧已安装 `dsh-bili-whale`，且 `~/.dsh/whale-diary.md` 已生成。

### 怎么拿目标 `session_id`

在 MaiBot WebUI →「聊天流」里点开要接收记忆的私聊/群聊，详情里能看到 `session_id`；
也可以查 `data/MaiBot.db` 的 `chat_sessions` 表。填进 `[target].target_session_id` 后重启 MaiBot。

> 聊天流重建（换账号、清库）后 `session_id` 会变，需要重新填。

---

## 三、安装

把仓库放进 `MaiBot/plugins/whale-memory-bridge/`，然后：

```bash
cd MaiBot/plugins/whale-memory-bridge
cp config.example.toml config.toml   # 仓库不提供含真实 session_id 的配置
# 编辑 config.toml：填 [target].target_session_id
```

在 WebUI「插件扩展」里启用（或重启 MaiBot）。日志出现这两行即在工作：

```
whale 记忆桥已启动
whale 外部记忆已更新: ~/.dsh/whale-memory.md（N 条）
```

---

## 四、配置（`config.toml`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `[source].whale_home` | `~/.dsh` | DSH 家目录 |
| `[source].diary_file` | `whale-diary.md` | 公开日记（「看过」表在这里） |
| `[source].private_diary_file` | `whale-diary-private.md` | 私密日记，默认不同步 |
| `[source].include_private_diary` | `false` | 是否把私密日记也写进记忆，**公开环境请保持 false** |
| `[target].target_session_id` | 空 | 记忆写入哪个聊天流；**空则不工作** |
| `[target].source_type` | `chat_summary` | 必须是 `chat_summary`，否则检索不到 |
| `[target].interval_seconds` | `300` | 后台增量扫描间隔 |
| `[target].max_items_per_run` | `20` | 单轮最多写几条，控制成本 |
| `[group].max_items` | `5` | 群聊被问「看过什么」时最多播报几条 |
| `[digest].limit` | `12` | 反向摘要取多少条 |

### 两个必须踩对的点

- **`source_type` 必须是 `chat_summary`**：A_Memorix 按 `source` 前缀解析检索作用域，
  换成 `whale_seen` 会入库但聊天里查不到；
- **清空 `data/a-memorix` 时必须同时删游标** `~/.dsh/whale-memory-bridge.cursor.json`，
  或用 `whale_memory_sync force=true`。

---

## 五、提供的工具

不注册聊天指令，全部由模型按需调用。

| 工具 | 用途 | 可见范围 |
|---|---|---|
| `whale_seen_recent` | 最近看过的视频标题 + 一句想法；已剔除写给主人的信与私密内容 | 群聊可用 |
| `whale_diary_recent` | 某天完整日记（信 / 心声 / 看过） | 私人，建议仅私聊 |
| `whale_memory_search` | 在 A_Memorix 里检索 whale 记忆 | 由 `include_private` 控制 |
| `whale_memory_context` | 把检索结果整理成可注入上下文的文本 | 同上 |
| `whale_memory_read` | 读反向摘要 `whale-memory.md` | 私人 |
| `whale_memory_sync` / `_digest` / `_status` | 手动同步 / 生成摘要 / 查看状态 | 管理用 |

### 为什么群聊只给「看过」

日记原文是写给「主人」的私人叙事（例如「你别把没看当成我没在」）。直接搬进群会跑出
人设，也会泄露只属于两个人的内容。所以群聊只播报「看过」，并清洗掉 `深读 / 只读字`
这类内部标记与「主人」称呼；信和心声只走私聊工具。

---

## 六、隐私与数据

- **只读** DSH 的文件，不修改 whale 的任何状态；
- 只写入你指定的聊天流，其他聊天流检索不到；
- `include_private_diary = false` 时 `whale-diary-private.md` 完全不读；
- 游标只记录已同步条目的 ID；
- 仓库里的默认配置**不含任何个人 session_id 或路径**，需要你自己填。

---

## 七、已知限制

- A_Memorix 必须配 embedding，否则只有 FTS，写入降级；
- 日记随日期增长，`whale_memory_digest` 的 prompt 会变长，注意成本；
- 依赖 DSH 与 dsh-bili-whale 的文件格式，上游改格式需同步改 `whale_source.py`；
- 反向摘要每轮被 whale 读取，属于「只读注入」，不会回写 whale 的状态。
