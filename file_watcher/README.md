# 文件监控 MCP Server 用户手册

## 一、产品介绍

### 1.1 产品概述

文件监控 MCP Server 是一个基于 MCP（Model Context Protocol）协议的文件监控服务，用于实时监控文件内容变化并通知用户。该服务支持延迟监控、文件删除自动恢复、多Session并发监控等特性，可无缝集成到 AI 客户端（如 OpenCode）中。

### 1.2 适用场景

| 场景     | 描述                  |
| ------ | ------------------- |
| 日志监控   | 实时监控日志文件变化，自动获取新增内容 |
| 配置文件监控 | 监控配置文件修改，及时响应配置变更   |
| 数据文件同步 | 监控数据文件更新，触发数据处理流程   |
| 开发调试   | 监控代码或输出文件，辅助开发调试    |
| 自动化任务  | 文件创建后自动触发处理任务       |

### 1.3 核心特性

| 特性       | 说明                                   |
| -------- | ------------------------------------ |
| **延迟监控** | 文件不存在时可预设监控，文件创建后自动激活                |
| **自动恢复** | 文件删除后自动暂停监控，恢复后自动重新激活                |
| **并发支持** | 多个Session可同时监控同一文件，所有Session都能收到事件通知 |
| **事件队列** | 采用事件队列轮询机制，确保文件变化事件不丢失               |
| **批量优化** | 多个监控同一文件时只读取一次，提高性能                  |
| **类型过滤** | 支持按事件类型（创建/更新/删除/恢复）过滤获取             |

### 1.4 系统架构

以下是文件监控 MCP Server 的整体架构图：

```
┌────────────────────────────────────────────────────────────────────┐
│                          MCP Client                                │
│                     (OpenCode/AI客户端)                            │
└────────────────────────────────────────────────────────────────────┘
                                │
                                │ MCP Protocol
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                  DelayedFileWatcherServer                          │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    MCP Tools Layer                           │  │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ │  │
│  │  │watch_file  │ │unwatch_file│ │list_watches│ │check_events│ │  │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘ │  │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ │  │
│  │  │get_content │ │stop_watcher│ │ get_stats  │ │cleanup_evt │ │  │
│  │  └────────────┘ └────────────┘ └────────────┘ └────────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                │                                   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                  State Management Layer                      │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │    WatchTarget                                          │ │  │
│  │  │    • file_path, callback_tool, status                   │ │  │
│  │  │    • status: active(正常) / pending(等待)               │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │    EventQueue (事件队列)                                │ │  │
│  │  │    • 容量: 1000条, 保留: 1小时                          │ │  │
│  │  │    • unretrieved / retrieved 状态标记                   │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │    file_last_contents (共享内容字典)                    │ │  │
│  │  │    • 文件路径 → 最后内容                                │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                │                                   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                 Event Handler Layer                          │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │    DelayedFileWatcherEventHandler                       │ │  │
│  │  │    • on_modified: 文件修改 + 目录修改(延迟激活)         │ │  │
│  │  │    • on_deleted: 文件删除处理                           │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │    Observer (watchdog)                                  │ │  │
│  │  │    • 监控目录，捕获文件系统事件                         │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
                                │
                                │ inotify
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                        File System                                 │
│                    (被监控的文件和目录)                            │
└────────────────────────────────────────────────────────────────────┘
```

**架构说明：**

| 层级                         | 说明                             |
| -------------------------- | ------------------------------ |
| **MCP Client**             | AI客户端（如OpenCode），通过MCP协议调用监控服务 |
| **MCP Tools Layer**        | 提供各类工具接口，如创建监控、获取事件、停止监控等      |
| **State Management Layer** | 管理监控目标状态、事件队列、共享内容字典           |
| **Event Handler Layer**    | 处理文件系统事件，包括修改、删除、延迟激活等         |
| **File System**            | 被监控的文件和目录，通过inotify机制监控        |

---

## 二、安装部署

### 2.1 系统要求

| 要求       | 说明                 |
| -------- | ------------------ |
| 操作系统     | Linux（版本不低于2.6.13） |
| Python版本 | Python 3.10+       |
| 依赖库      | mcp, watchdog      |

### 2.2 安装依赖

```bash
# 安装 MCP SDK
pip install mcp

# 安装 watchdog（文件监控库）
pip install watchdog
```

### 2.3 配置 MCP 客户端

#### 方式一：全局配置

在用户级配置文件 `~/.config/opencode/config.json` 中添加：

```json
{
  "mcpServers": {
    "file-watcher": {
      "command": "python",
      "args": ["/home/project/file_watcher_server_debug/file_watcher_server.py"]
    }
  }
}
```

#### 方式二：项目级配置

在项目目录 `.opencode/config.json` 中添加：

```json
{
  "mcp": {
    "servers": {
      "file-watcher": {
        "command": "python",
        "args": ["./file_watcher_server.py"]
      }
    }
  }
}
```

### 2.4 验证安装

启动 MCP 客户端后，调用 `list_watches` 工具验证服务是否正常运行：

```
调用：list_watches()
预期返回：监控列表（初始为空）
```

---

## 三、工具使用指南

### 3.1 工具总览

| 工具名称               | 功能描述           |
| ------------------ | -------------- |
| `watch_file`       | 开始监控指定文件       |
| `unwatch_file`     | 停止指定监控         |
| `list_watches`     | 列出所有监控         |
| `get_file_content` | 获取文件当前内容       |
| `check_events`     | 检查事件队列，获取未处理事件 |
| `cleanup_events`   | 清理过期事件         |
| `get_stats`        | 获取服务器统计信息      |
| `stop_watcher`     | 停止所有监控并关闭服务器   |

---

### 3.2 watch_file - 创建文件监控

#### 功能说明

创建对指定文件的监控。支持延迟监控：如果文件不存在，会监控文件所在目录，文件创建后自动激活监控。

#### 参数说明

| 参数              | 类型     | 必需  | 说明                       |
| --------------- | ------ | --- | ------------------------ |
| `file_path`     | string | 是   | 文件绝对路径（文件可不存在，但父级目录必须存在） |
| `callback_tool` | string | 是   | 文件更新后回调的工具名称             |
| `encoding`      | string | 否   | 文件编码，默认 utf-8            |

#### 使用示例

**示例1：监控已存在的文件**

```
调用：
watch_file(
  file_path="/home/logs/app.log",
  callback_tool="process_log_update"
)

返回：
监控已启动
{
  "watch_id": "watch_a1b2c3d4_1",
  "file_path": "/home/logs/app.log",
  "callback_tool": "process_log_update",
  "status": "active",
  "file_exists": true,
  "initial_content_length": 1024,
  "message": "监控已启动"
}
```

**示例2：监控不存在的文件（延迟监控）**

```
调用：
watch_file(
  file_path="/home/data/output.json",
  callback_tool="handle_data_output"
)

返回：
延迟监控已创建，等待文件创建
{
  "watch_id": "watch_a1b2c3d4_2",
  "file_path": "/home/data/output.json",
  "callback_tool": "handle_data_output",
  "status": "pending",
  "file_exists": false,
  "initial_content_length": 0,
  "message": "延迟监控已创建，等待文件创建"
}
```

**示例3：父级目录不存在时的错误提示**

```
调用：
watch_file(
  file_path="/nonexistent/path/file.txt",
  callback_tool="my_callback"
)

返回：
错误: 父级目录不存在

文件路径: /nonexistent/path/file.txt
父级目录: /nonexistent/path
状态: 目录不存在

解决方案:
1. 先创建父级目录:
   mkdir -p /nonexistent/path
2. 然后重新调用:
   watch_file(file_path=/nonexistent/path/file.txt, callback_tool=my_callback)
```

#### 监控状态说明

| 状态        | 说明                  |
| --------- | ------------------- |
| `active`  | 文件存在，正常监控中          |
| `pending` | 文件不存在或已删除，等待文件创建/恢复 |

#### 重要提示

1. **父级目录必须存在**：否则会返回错误提示和解决方案
2. **文件路径使用绝对路径**：避免路径歧义
3. **callback_tool 建议**：指定一个实际存在的 MCP 工具名，用于事件处理

---

### 3.3 check_events - 获取文件变化事件

#### 功能说明

检查事件队列，获取未被处理的文件变化事件。这是获取文件变化通知的核心方式，建议定期轮询调用。

#### 参数说明

| 参数               | 类型      | 必需  | 说明                       |
| ---------------- | ------- | --- | ------------------------ |
| `limit`          | integer | 否   | 返回事件数量上限，默认 10           |
| `mark_retrieved` | boolean | 否   | 是否标记为已检索（下次不再返回），默认 true |
| `event_types`    | array   | 否   | 过滤事件类型，默认返回所有类型          |

#### 事件类型

| 事件类型            | 说明                       |
| --------------- | ------------------------ |
| `file_created`  | 文件首次创建（pending → active） |
| `file_updated`  | 文件内容更新（active → active）  |
| `file_deleted`  | 文件被删除（active → pending）  |
| `file_restored` | 文件恢复创建（pending → active） |

#### 使用示例

**示例1：获取所有未处理事件**

```
调用：
check_events()

返回：
文件变化事件:
{
  "total_events": 2,
  "mark_retrieved": true,
  "message": "返回 2 个事件。已标记为检索，下次调用不再返回。",
  "events": [
    {
      "event_id": "event_a1b2c3_1783062403_1",
      "watch_id": "watch_a1b2c3d4_1",
      "file_path": "/home/logs/app.log",
      "event_type": "file_updated",
      "content_length": 2048,
      "callback_tool": "process_log_update",
      "timestamp": 1783062403.5
    },
    {
      "event_id": "event_a1b2c3_1783062405_2",
      "watch_id": "watch_a1b2c3d4_2",
      "file_path": "/home/data/output.json",
      "event_type": "file_created",
      "content_length": 512,
      "callback_tool": "handle_data_output",
      "timestamp": 1783062405.8
    }
  ],
  "tip": "建议对每个事件执行 callback_tool 进行处理。内容可通过 get_file_content 获取。"
}
```

**示例2：只获取文件更新事件**

```
调用：
check_events(
  event_types=["file_updated"]
)

返回：仅包含 event_type="file_updated" 的事件
```

**示例3：获取事件但不标记为已检索**

```
调用：
check_events(
  mark_retrieved=false
)

说明：下次调用仍会返回相同事件（用于重复处理场景）
```

**示例4：无新事件时**

```
调用：
check_events()

返回：
事件队列中暂无新事件。
建议：定期调用此工具（如每5-10秒）以获取文件变化通知。
```

#### 推荐轮询频率

| 场景     | 建议频率   |
| ------ | ------ |
| 高频文件更新 | 每 5 秒  |
| 一般监控   | 每 10 秒 |
| 低频监控   | 每 30 秒 |

---

### 3.4 unwatch_file - 停止文件监控

#### 功能说明

停止指定的文件监控，包括延迟监控（pending状态）。

#### 参数说明

| 参数         | 类型     | 必需  | 说明                     |
| ---------- | ------ | --- | ---------------------- |
| `watch_id` | string | 是   | 监控 ID（由 watch_file 返回） |

#### 使用示例

```
调用：
unwatch_file(watch_id="watch_a1b2c3d4_1")

返回：
监控已取消: watch_a1b2c3d4_1
文件: /home/logs/app.log
状态: active
```

---

### 3.5 list_watches - 列出所有监控

#### 功能说明

列出当前所有监控，包括活跃监控和延迟监控。

#### 参数说明

| 参数             | 类型      | 必需  | 说明               |
| -------------- | ------- | --- | ---------------- |
| `show_pending` | boolean | 否   | 是否显示延迟监控，默认 true |

#### 使用示例

```
调用：
list_watches()

返回：
监控列表:
{
  "total_watches": 3,
  "active_watches": 2,
  "pending_watches": 1,
  "directory_watchers": 2
}

详细列表:
[
  {
    "watch_id": "watch_a1b2c3d4_1",
    "file_path": "/home/logs/app.log",
    "callback_tool": "process_log_update",
    "status": "active",
    "file_exists": true
  },
  {
    "watch_id": "watch_a1b2c3d4_2",
    "file_path": "/home/data/output.json",
    "callback_tool": "handle_data_output",
    "status": "pending",
    "file_exists": false
  }
]
```

---

### 3.6 get_file_content - 获取文件内容

#### 功能说明

获取指定文件的当前完整内容。

#### 参数说明

| 参数          | 类型     | 必需  | 说明            |
| ----------- | ------ | --- | ------------- |
| `file_path` | string | 是   | 文件绝对路径        |
| `encoding`  | string | 否   | 文件编码，默认 utf-8 |

#### 使用示例

```
调用：
get_file_content(file_path="/home/logs/app.log")

返回：
文件内容:
{
  "file_path": "/home/logs/app.log",
  "content": "日志内容...",
  "length": 2048
}
```

---

### 3.7 get_stats - 获取服务器统计信息

#### 功能说明

获取监控服务器的运行统计信息。

#### 使用示例

```
调用：
get_stats()

返回：
监控服务器统计信息:
{
  "process_id": "a1b2c3d4",
  "total_watches": 5,
  "active_watches": 3,
  "pending_watches": 2,
  "directory_watch_count": 3,
  "observer_running": true,
  "event_queue": {
    "total_events": 50,
    "unretrieved_events": 5,
    "max_size": 1000,
    "max_age_seconds": 3600
  },
  "features": {
    "delayed_watch_support": true,
    "multi_session_support": true,
    "batch_processing": true,
    "auto_activation": true,
    "event_queue_support": true
  }
}
```

---

### 3.8 cleanup_events - 清理过期事件

#### 功能说明

手动清理事件队列中的过期事件（超过1小时的旧事件）。

#### 使用示例

```
调用：
cleanup_events()

返回：
事件清理结果:
{
  "cleaned_count": 15,
  "remaining_events": 35,
  "message": "清理了 15 个过期事件（超过1小时的旧事件）。"
}
```

---

### 3.9 stop_watcher - 停止所有监控

#### 功能说明

停止所有文件监控并关闭监控服务器。

#### 使用示例

```
调用：
stop_watcher()

返回：
文件监控器已停止，所有监控已清除
进程: a1b2c3d4
```

---

## 四、典型使用场景

### 4.1 场景一：监控日志文件实时更新

**需求**：监控应用日志文件，实时获取新增日志内容。

**操作步骤**：

```
1. 创建监控：
   watch_file(
     file_path="/var/log/app.log",
     callback_tool="analyze_log"
   )

2. 定期轮询事件（每10秒）：
   check_events()

3. 获取到事件后处理：
   - 根据 event_type 判断变化类型
   - 使用 get_file_content 获取完整内容
   - 执行 callback_tool 进行分析

4. 停止监控（可选）：
   unwatch_file(watch_id="watch_xxx")
```

---

### 4.2 场景二：等待文件创建后自动处理

**需求**：一个数据处理任务会生成输出文件，需要监控该文件的创建。

**操作步骤**：

```
1. 创建延迟监控（文件不存在）：
   watch_file(
     file_path="/home/output/result.json",
     callback_tool="process_result"
   )

   返回：status="pending"

2. 定期轮询事件：
   check_events()

3. 文件创建后自动激活：
   返回事件：event_type="file_created"
   监控状态：pending → active

4. 执行处理：
   get_file_content(file_path="/home/output/result.json")
   执行 callback_tool 处理数据
```

---

### 4.3 场景三：文件删除恢复后自动重新监控

**需求**：监控一个可能被临时删除并重新创建的配置文件。

**操作步骤**：

```
1. 创建监控：
   watch_file(
     file_path="/home/config/settings.yaml",
     callback_tool="reload_config"
   )

2. 定期轮询事件：
   check_events()

3. 文件删除时：
   返回事件：event_type="file_deleted"
   监控状态：active → pending
   继续监控目录，等待恢复

4. 文件恢复时：
   返回事件：event_type="file_restored"
   监控状态：pending → active
   自动恢复正常监控
```

---

### 4.4 场景四：多Session并发监控同一文件

**需求**：多个AI Session同时监控同一个日志文件，各自独立处理。

**特点**：每个Session创建独立监控，收到相同的事件通知。

**Session A 操作**：

```
watch_file(
  file_path="/var/log/system.log",
  callback_tool="session_a_handler"
)
返回：watch_id="watch_a1b2c3d4_1"
```

**Session B 操作**：

```
watch_file(
  file_path="/var/log/system.log",
  callback_tool="session_b_handler"
)
返回：watch_id="watch_a1b2c3d4_2"
```

**文件更新时**：

- 两个Session都能收到事件
- 各自独立执行各自的 callback_tool

---

### 4.5 场景五：按事件类型过滤监控

**需求**：只关心文件的创建和恢复事件，不关心更新事件。

**操作步骤**：

```
1. 创建监控：
   watch_file(
     file_path="/home/data/input.csv",
     callback_tool="import_data"
   )

2. 定期轮询，只获取创建/恢复事件：
   check_events(
     event_types=["file_created", "file_restored"]
   )
```

---

## 五、最佳实践

### 5.1 轮询策略

| 策略            | 说明                         |
| ------------- | -------------------------- |
| **定期轮询**      | 建议每5-10秒调用一次 check_events  |
| **及时处理**      | 获取事件后尽快处理，避免队列积压           |
| **合理设置limit** | 根据实际需求设置 limit，避免一次性获取过多事件 |

### 5.2 监控管理

| 建议             | 说明                       |
| -------------- | ------------------------ |
| **记录watch_id** | 创建监控后保存 watch_id，用于后续管理  |
| **及时清理**       | 不再需要的监控及时调用 unwatch_file |
| **定期检查**       | 使用 list_watches 检查监控状态   |

### 5.3 错误处理

| 场景      | 处理方式                     |
| ------- | ------------------------ |
| 父级目录不存在 | 按返回提示先创建目录，再重新创建监控       |
| 文件编码错误  | 指定正确的 encoding 参数        |
| 事件队列满   | 调用 cleanup_events 清理过期事件 |

---

## 六、常见问题解答

### Q1：文件不存在时能创建监控吗？

**答**：可以。系统支持延迟监控，只要父级目录存在即可。文件创建后会自动激活监控，状态从 `pending` 变为 `active`。

### Q2：父级目录不存在怎么办？

**答**：系统会返回错误提示和解决方案。需要先创建父级目录：

```bash
mkdir -p /path/to/directory
```

然后重新调用 `watch_file`。

### Q3：文件删除后监控会失效吗？

**答**：不会。监控会自动暂停（状态变为 `pending`），继续监控目录。文件恢复后会自动重新激活。

### Q4：多个Session监控同一文件会有冲突吗？

**答**：不会。每个Session创建独立监控，文件变化时所有Session都能收到事件通知。

### Q5：事件会丢失吗？

**答**：不会。系统采用事件队列机制，事件存储在队列中等待轮询获取。事件保留1小时，最多存储1000条。

### Q6：如何避免重复处理同一事件？

**答**：调用 `check_events` 时使用默认参数 `mark_retrieved=true`，事件被获取后会标记为已检索，下次不再返回。

### Q7：如何获取文件完整内容？

**答**：事件返回中不包含完整内容（避免数据过大）。使用 `get_file_content` 工具获取文件完整内容。

### Q8：callback_tool 参数有什么用？

**答**：指定文件变化时要回调的工具名称。事件返回中会包含此名称，提示用户应执行该工具处理事件。

---

## 七、注意事项

### 7.1 路径要求

- 使用**绝对路径**，避免路径歧义
- 父级目录必须存在（否则返回错误）
- 文件可不存在（支持延迟监控）

### 7.2 性能考虑

- 同一文件多个监控时，系统会批量处理，只读取一次
- 事件队列有容量限制（1000条），定期清理过期事件
- 避免过高频轮询（建议5-10秒间隔）

### 7.3 事件处理

- 获取事件后及时处理，避免队列积压
- 使用 `mark_retrieved=true` 避免重复处理
- 通过 `event_type` 区分事件类型，采取不同处理策略

### 7.4 监控生命周期

| 阶段   | 状态             | 说明              |
| ---- | -------------- | --------------- |
| 创建监控 | active/pending | 根据文件是否存在决定      |
| 文件更新 | active         | 正常监控中           |
| 文件删除 | pending        | 暂停监控，等待恢复       |
| 文件恢复 | active         | 自动重新激活          |
| 取消监控 | -              | 调用 unwatch_file |

---

## 八、附录

### 8.1 事件类型对照表

| event_type      | 触发条件   | 状态变化             |
| --------------- | ------ | ---------------- |
| `file_created`  | 文件首次创建 | pending → active |
| `file_updated`  | 文件内容修改 | active → active  |
| `file_deleted`  | 文件被删除  | active → pending |
| `file_restored` | 文件恢复创建 | pending → active |

### 8.2 监控状态说明

| status    | 说明             |
| --------- | -------------- |
| `active`  | 文件存在，正常监控中     |
| `pending` | 文件不存在或已删除，等待激活 |

### 8.3 事件队列参数

| 参数              | 值    | 说明          |
| --------------- | ---- | ----------- |
| max_size        | 1000 | 最大事件数量      |
| max_age_seconds | 3600 | 事件保留时间（1小时） |

### 8.4 依赖说明

| 依赖              | 版本要求  | 说明      |
| --------------- | ----- | ------- |
| Python          | 3.10+ | 运行环境    |
| python-mcp      | -     | MCP SDK |
| python-watchdog | -     | 文件系统监控库 |

---

## 九、技术支持

如遇到问题，可按以下步骤排查：

1. 检查服务状态：调用 `get_stats`
2. 查看监控列表：调用 `list_watches`
3. 检查事件队列：调用 `check_events`
4. 清理过期事件：调用 `cleanup_events`
5. 重启监控服务：调用 `stop_watcher` 后重新配置

---

**版本**: v0.1  
**更新日期**: 2026-07-04  
