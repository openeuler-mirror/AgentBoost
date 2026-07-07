#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2013-2026. All rights reserved.
# Licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2026-07-04
# Description: file-watcher MCP server.

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from mcp import types

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class WatchTarget:
    """监控目标配置"""
    file_path: str
    callback_tool: str
    process_id: str = ""
    watch_id: str = ""
    enabled: bool = True
    last_content: Optional[str] = None
    encoding: str = 'utf-8'
    status: str = "active"  # active, pending（延迟监控）


@dataclass
class FileEvent:
    """
    文件变化事件

    用于持久化存储文件变化事件，确保事件不丢失。
    Agent 通过 check_events 工具轮询获取事件。
    """
    event_id: str           # 唯一标识
    watch_id: str           # 关联的监控ID
    file_path: str          # 文件路径
    event_type: str         # file_created/file_updated/file_deleted/file_restored
    content: str            # 文件内容（file_deleted时为空）
    content_length: int     # 内容长度
    callback_tool: str      # 回调工具名
    process_id: str         # 进程ID
    timestamp: float        # 事件时间戳
    retrieved: bool = False # 是否已被轮询获取


class EventQueue:
    """
    事件队列

    特性：
    1. 带容量限制，防止内存溢出
    2. 按时间排序，最新事件在前
    3. 支持事件状态标记（retrieved）
    4. 自动清理过期事件
    """

    def __init__(self, max_size: int = 1000, max_age_seconds: int = 3600):
        self.events: List[FileEvent] = []
        self.max_size = max_size
        self.max_age_seconds = max_age_seconds
        self._event_id_counter = 0

    def add(self, event: FileEvent) -> str:
        """
        添加事件到队列

        返回: event_id
        """
        # 容量检查：超出时移除最旧的已检索事件
        if len(self.events) >= self.max_size:
            # 优先移除已检索的旧事件
            old_events = [e for e in self.events if e.retrieved]
            if old_events:
                # 移除最旧的10%
                remove_count = max(1, len(old_events) // 10)
                for _ in range(remove_count):
                    if old_events:
                        oldest = min(old_events, key=lambda e: e.timestamp)
                        self.events.remove(oldest)
                        old_events.remove(oldest)
            else:
                # 如果没有已检索事件，移除最旧的未检索事件
                oldest = min(self.events, key=lambda e: e.timestamp)
                self.events.remove(oldest)

        self.events.append(event)
        logger.info(f"事件已添加到队列: {event.event_id}, 类型: {event.event_type}, 文件: {event.file_path}")
        return event.event_id

    def get_unretrieved(self, limit: int = 10,
                        event_types: Optional[List[str]] = None) -> List[FileEvent]:
        """
        获取未检索的事件

        参数:
            limit: 返回事件数量上限
            event_types: 过滤事件类型（可选）

        返回:
            事件列表（按时间倒序）
        """
        unretrieved = [e for e in self.events if not e.retrieved]

        # 类型过滤
        if event_types:
            unretrieved = [e for e in unretrieved if e.event_type in event_types]

        # 按时间倒序，取前limit个
        unretrieved.sort(key=lambda e: e.timestamp, reverse=True)
        return unretrieved[:limit]

    def mark_retrieved(self, event_ids: List[str]):
        """标记事件为已检索"""
        for event_id in event_ids:
            for event in self.events:
                if event.event_id == event_id:
                    event.retrieved = True
                    logger.info(f"事件已标记为检索: {event_id}")

    def cleanup_old_events(self) -> int:
        """
        清理过期事件

        返回: 清理的事件数量
        """
        import time
        current_time = time.time()
        threshold = current_time - self.max_age_seconds

        old_events = [e for e in self.events if e.timestamp < threshold]
        for event in old_events:
            self.events.remove(event)

        if old_events:
            logger.info(f"清理了 {len(old_events)} 个过期事件")

        return len(old_events)

    def get_stats(self) -> Dict[str, int]:
        """获取队列统计信息"""
        return {
            "total_events": len(self.events),
            "unretrieved_events": sum(1 for e in self.events if not e.retrieved),
            "max_size": self.max_size,
            "max_age_seconds": self.max_age_seconds
        }

    def generate_event_id(self, process_id: str) -> str:
        """生成唯一事件ID"""
        self._event_id_counter += 1
        import time
        return f"event_{process_id[:8]}_{int(time.time())}_{self._event_id_counter}"


class DelayedFileWatcherEventHandler(FileSystemEventHandler):
    """
    文件系统事件处理器（支持延迟监控）
    """

    def __init__(self, watcher: 'DelayedFileWatcherServer',
                 loop: asyncio.AbstractEventLoop = None):
        super().__init__()
        self.watcher = watcher
        self.loop = loop

    def on_modified(self, event):
        """
        文件修改事件处理
        """
        modified_path = os.path.abspath(event.src_path)

        if event.is_directory:
            logger.info(f"检测到目录修改: {modified_path}")
            self._check_pending_watches(modified_path)
            return

        logger.info(f"检测到文件修改: {modified_path}")

        # 收集所有匹配的监控目标
        matched_targets = []
        for watch_id, target in self.watcher.watch_targets.items():
            target_path = os.path.abspath(target.file_path)
            if target_path == modified_path and target.status == "active":
                logger.info(f"匹配到监控目标: {watch_id}, callback: {target.callback_tool}")
                matched_targets.append((watch_id, target))

        # 批量处理所有匹配的目标
        if matched_targets:
            logger.info(f"文件 {modified_path} 有 {len(matched_targets)} 个监控目标")

            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(
                        self.watcher._handle_file_update_batch(modified_path, matched_targets)
                    )
                )
            else:
                logger.warning("没有运行的事件循环，无法处理文件更新")

    def on_deleted(self, event):
        """
        文件删除事件处理
        """
        if event.is_directory:
            # 目录删除：清理该目录下的所有监控
            deleted_dir = os.path.abspath(event.src_path)
            logger.info(f"检测到目录删除: {deleted_dir}")

            # 查找该目录下的所有监控
            watches_to_remove = []
            for watch_id, target in self.watcher.watch_targets.items():
                target_dir = os.path.dirname(os.path.abspath(target.file_path))
                if target_dir == deleted_dir:
                    watches_to_remove.append(watch_id)

            # 清理监控（同步操作）
            for watch_id in watches_to_remove:
                self.watcher.watch_targets.pop(watch_id, None)
                logger.info(f"监控已清理（目录删除）: {watch_id}")

            return

        # 文件删除处理
        deleted_path = os.path.abspath(event.src_path)
        logger.info(f"检测到文件删除: {deleted_path}")

        # 查找匹配的监控
        matched_targets = []
        for watch_id, target in self.watcher.watch_targets.items():
            target_path = os.path.abspath(target.file_path)
            if target_path == deleted_path and target.status == "active":
                matched_targets.append((watch_id, target))

        # 处理文件删除
        if matched_targets:
            logger.info(f"文件 {deleted_path} 有 {len(matched_targets)} 个监控")

            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(
                        self._handle_file_deletion(deleted_path, matched_targets)
                    )
                )
            else:
                # 同步暂停（事件循环不可用时）
                self._pause_watches_sync(matched_targets)

    def _check_pending_watches(self, directory_path: str):
        """
        检查延迟监控的目标文件是否已创建

        参数:
            directory_path: 目录路径（触发修改事件的目录）
        """
        # 查找该目录下的所有延迟监控（pending状态）
        pending_targets = []
        for watch_id, target in self.watcher.watch_targets.items():
            if target.status != "pending":
                continue

            # 检查目标文件是否在该目录下
            target_dir = os.path.dirname(os.path.abspath(target.file_path))
            if target_dir == directory_path:
                # 检查文件是否存在
                if os.path.isfile(target.file_path):
                    pending_targets.append((watch_id, target))
                    logger.info(f"延迟监控的目标文件已创建: {target.file_path}")

        # 激活延迟监控
        if pending_targets:
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(
                        self._activate_pending_watches(pending_targets)
                    )
                )
            else:
                logger.warning("没有运行的事件循环，无法激活延迟监控")

    async def _activate_pending_watches(self, pending_targets: List[Tuple[str, WatchTarget]]):
        """
        激活延迟监控

        参数:
            pending_targets: 延迟监控列表 [(watch_id, target), ...]
        """
        for watch_id, target in pending_targets:
            try:
                # 读取文件初始内容
                with open(target.file_path, 'r', encoding=target.encoding) as f:
                    initial_content = f.read()

                # 记录旧状态
                old_status = target.status

                # 激活监控
                target.status = "active"
                target.last_content = initial_content

                # 更新共享的file_last_contents
                self.watcher.file_last_contents[target.file_path] = initial_content

                logger.info(f"监控已激活: {watch_id}, 状态: {old_status} → active, 文件: {target.file_path}")

                # 触发回调
                event_type = "file_created" if old_status == "pending" else "file_restored"

                await self.watcher._invoke_callback(
                    watch_id,
                    target.callback_tool,
                    target.file_path,
                    initial_content,
                    target.process_id,
                    event_type=event_type  # 区分file_created和file_restored
                )

                # 检查是否需要取消目录监控
                await self._cleanup_directory_watch(target.file_path)

            except Exception as e:
                logger.error(f"激活延迟监控失败: {watch_id}, 错误: {e}")

    async def _handle_file_deletion(self, file_path: str,
                                     matched_targets: List[Tuple[str, WatchTarget]]):
        """
        处理文件删除

        状态转换：active → pending

        参数:
            file_path: 删除的文件路径
            matched_targets: 匹配的监控列表
        """
        for watch_id, target in matched_targets:
            # 状态转换：active → pending
            old_status = target.status
            target.status = "pending"
            target.last_content = None

            # 清理file_last_contents
            if file_path in self.watcher.file_last_contents:
                del self.watcher.file_last_contents[file_path]

            logger.info(f"文件删除，监控暂停: {watch_id}, 状态: {old_status} → pending")

            # 触发回调通知
            await self.watcher._invoke_callback(
                watch_id,
                target.callback_tool,
                file_path,
                "",  # 空内容
                target.process_id,
                event_type="file_deleted"  # 特殊标记
            )

    def _pause_watches_sync(self, matched_targets: List[Tuple[str, WatchTarget]]):
        """
        同步暂停监控（事件循环不可用时）

        参数:
            matched_targets: 匹配的监控列表
        """
        for watch_id, target in matched_targets:
            target.status = "pending"
            target.last_content = None

            # 清理file_last_contents
            if target.file_path in self.watcher.file_last_contents:
                del self.watcher.file_last_contents[target.file_path]

            logger.info(f"监控暂停（同步）: {watch_id}")

    async def _cleanup_directory_watch(self, file_path: str):
        """
        清理目录监控（如果目录下没有其他延迟监控）

        参数:
            file_path: 刚创建的文件路径
        """
        file_dir = os.path.dirname(os.path.abspath(file_path))

        # 检查该目录下是否还有其他延迟监控（pending状态）
        has_pending = False
        for watch_id, target in self.watcher.watch_targets.items():
            if target.status == "pending":
                target_dir = os.path.dirname(os.path.abspath(target.file_path))
                if target_dir == file_dir:
                    has_pending = True
                    logger.info(f"目录 {file_dir} 还有其他延迟监控，保持目录监控")
                    break

        # 如果没有其他延迟监控，检查是否还有活跃的文件监控
        if not has_pending:
            has_active = False
            for watch_id, target in self.watcher.watch_targets.items():
                if target.status == "active":
                    target_dir = os.path.dirname(os.path.abspath(target.file_path))
                    if target_dir == file_dir:
                        has_active = True
                        break

            # 如果既没有延迟监控，也没有活跃监控，取消目录监控
            if not has_active and file_dir in self.watcher.directory_watchers:
                # 注意：这里不取消，因为可能还有其他文件需要监控
                # 实际应该根据需求决定是否取消
                logger.info(f"目录 {file_dir} 无监控需要，可以取消目录监控（可选）")


class DelayedFileWatcherServer:
    """
    文件监控 MCP 服务器（支持延迟监控）
    """

    def __init__(self, name: str = "file-watcher-server-delayed"):
        self.name = name
        self.server = Server(name)

        # 进程唯一标识
        self.process_id = uuid.uuid4().hex

        # 本地状态
        self.observer: Optional[Observer] = None
        self.event_handler: Optional[DelayedFileWatcherEventHandler] = None
        self.watch_targets: Dict[str, WatchTarget] = {}
        self.watch_id_counter = 0
        self.directory_watchers: Dict[str, Any] = {}

        # 文件路径 -> 最后内容的共享字典
        self.file_last_contents: Dict[str, str] = {}

        # 事件队列（持久化存储，防止事件丢失）
        self.event_queue = EventQueue(
            max_size=1000,      # 最大事件数
            max_age_seconds=3600  # 事件保留1小时
        )

        # 注册MCP工具和处理器
        self._setup_tools()
        self._setup_handlers()

        logger.info(f"延迟监控文件监控服务器初始化完成，进程ID: {self.process_id[:8]}")

    def _setup_tools(self):
        """设置MCP工具定义"""

        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="watch_file",
                    description=(
                        "开始监控指定文件。支持延迟监控：如果文件不存在，"
                        "会监控文件所在目录，文件创建后自动激活监控。"
                        "注意：父级目录必须存在，否则会返回错误提示。"
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "要监控的文件绝对路径（文件可以不存在，但父级目录必须存在）"
                            },
                            "callback_tool": {
                                "type": "string",
                                "description": "文件更新后要回调的 MCP 工具名称"
                            },
                            "encoding": {
                                "type": "string",
                                "description": "文件编码，默认为 utf-8",
                                "default": "utf-8"
                            }
                        },
                        "required": ["file_path", "callback_tool"]
                    }
                ),
                Tool(
                    name="unwatch_file",
                    description="停止监控指定文件（包括延迟监控）",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "watch_id": {
                                "type": "string",
                                "description": "监控 ID"
                            }
                        },
                        "required": ["watch_id"]
                    }
                ),
                Tool(
                    name="list_watches",
                    description="列出当前所有监控（包括延迟监控）",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "show_pending": {
                                "type": "boolean",
                                "description": "是否显示延迟监控（pending状态），默认true",
                                "default": True
                            }
                        }
                    }
                ),
                Tool(
                    name="get_file_content",
                    description="获取指定文件的当前内容",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "文件绝对路径"
                            },
                            "encoding": {
                                "type": "string",
                                "description": "文件编码，默认为 utf-8",
                                "default": "utf-8"
                            }
                        },
                        "required": ["file_path"]
                    }
                ),
                Tool(
                    name="stop_watcher",
                    description="停止所有文件监控并关闭监控器",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="get_stats",
                    description="获取监控服务器统计信息",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="check_events",
                    description=(
                        "检查文件变化事件队列（轮询机制）。"
                        "返回未被处理的文件变化事件列表。"
                        "建议定期调用此工具（如每5-10秒）以获取文件变化通知。"
                        "事件类型包括：file_created、file_updated、file_deleted、file_restored。"
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "返回事件数量上限，默认10",
                                "default": 10
                            },
                            "mark_retrieved": {
                                "type": "boolean",
                                "description": "是否标记事件为已检索（下次调用不再返回），默认true",
                                "default": True
                            },
                            "event_types": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "过滤事件类型：file_created、file_updated、file_deleted、file_restored。默认返回所有类型。",
                                "default": None
                            }
                        }
                    }
                ),
                Tool(
                    name="cleanup_events",
                    description="清理过期的事件队列条目（超过1小时的旧事件）",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                )
            ]

    def _setup_handlers(self):
        """设置MCP工具调用处理器"""

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
            try:
                if name == "watch_file":
                    return await self._handle_watch_file(arguments)
                elif name == "unwatch_file":
                    return await self._handle_unwatch_file(arguments)
                elif name == "list_watches":
                    return await self._handle_list_watches(arguments)
                elif name == "get_file_content":
                    return await self._handle_get_file_content(arguments)
                elif name == "stop_watcher":
                    return await self._handle_stop_watcher(arguments)
                elif name == "get_stats":
                    return await self._handle_get_stats(arguments)
                elif name == "check_events":
                    return await self._handle_check_events(arguments)
                elif name == "cleanup_events":
                    return await self._handle_cleanup_events(arguments)
                else:
                    return [TextContent(type="text", text=f"错误: 未知工具 '{name}'")]
            except Exception as e:
                logger.error(f"工具调用错误: {name}", exc_info=True)
                return [TextContent(type="text", text=f"工具调用错误: {str(e)}")]

    async def _handle_watch_file(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """
        处理watch_file工具调用
        """
        file_path = arguments.get("file_path")
        callback_tool = arguments.get("callback_tool")
        encoding = arguments.get("encoding", "utf-8")

        # 验证参数
        if not file_path:
            return [TextContent(type="text", text="错误: 缺少 file_path 参数")]

        if not callback_tool:
            return [TextContent(type="text", text="错误: 缺少 callback_tool 参数")]

        # 检查父级目录是否存在
        file_dir = os.path.dirname(os.path.abspath(file_path))
        dir_exists = os.path.isdir(file_dir)

        if not dir_exists:
            # 父级目录不存在，返回友好提示
            return [TextContent(
                type="text",
                text=f"错误: 父级目录不存在\n\n"
                     f"文件路径: {file_path}\n"
                     f"父级目录: {file_dir}\n"
                     f"状态: 目录不存在\n\n"
                     f"解决方案:\n"
                     f"1. 先创建父级目录:\n"
                     f"   mkdir -p {file_dir}\n"
                     f"2. 然后重新调用:\n"
                     f"   watch_file(file_path={file_path}, callback_tool={callback_tool})\n\n"
                     f"注意: 当前版本不支持父级目录不存在时的延迟监控。\n"
                     f"请确保父级目录存在后再创建监控。"
            )]

        # 检查文件是否存在
        file_exists = os.path.isfile(file_path)

        # 生成watch_id
        self.watch_id_counter += 1
        watch_id = f"watch_{self.process_id[:8]}_{self.watch_id_counter}"

        # 根据文件是否存在设置状态
        if file_exists:
            # 文件存在：正常监控
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    initial_content = f.read()
            except Exception as e:
                return [TextContent(type="text", text=f"错误: 无法读取文件: {str(e)}")]

            status = "active"
            message = "监控已启动"
        else:
            # 文件不存在：延迟监控
            initial_content = None
            status = "pending"
            message = "延迟监控已创建，等待文件创建"
            logger.info(f"文件不存在，创建延迟监控: {file_path}")

        # 存储监控状态
        self.watch_targets[watch_id] = WatchTarget(
            file_path=file_path,
            callback_tool=callback_tool,
            process_id=self.process_id,
            watch_id=watch_id,
            enabled=True,
            last_content=initial_content,
            encoding=encoding,
            status=status
        )

        # 如果文件存在，更新共享的file_last_contents
        if file_exists:
            self.file_last_contents[file_path] = initial_content

        # 监控目录
        file_dir = os.path.dirname(os.path.abspath(file_path))

        # 初始化observer（如果尚未启动）
        if self.observer is None:
            self.observer = Observer()
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            self.event_handler = DelayedFileWatcherEventHandler(self, loop)
            self.observer.start()
            logger.info("inotify监控器已启动")

        # 监控目录（如果尚未监控）
        if file_dir not in self.directory_watchers:
            watch = self.observer.schedule(
                self.event_handler,
                file_dir,
                recursive=False
            )
            self.directory_watchers[file_dir] = watch
            logger.info(f"开始监控目录: {file_dir}")

        # 返回结果
        result = {
            "watch_id": watch_id,
            "file_path": file_path,
            "callback_tool": callback_tool,
            "process_id": self.process_id[:8],
            "status": status,
            "file_exists": file_exists,
            "initial_content_length": len(initial_content) if initial_content else 0,
            "message": message,
            "features": [
                "延迟监控支持",
                "多session并发监控支持",
                "批量处理文件更新"
            ]
        }

        logger.info(f"监控创建完成: {watch_id}, 状态: {status}, 文件: {file_path}")

        return [TextContent(
            type="text",
            text=f"{message}\n{json.dumps(result, ensure_ascii=False, indent=2)}"
        )]

    async def _handle_unwatch_file(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """处理unwatch_file工具调用（包括延迟监控）"""
        watch_id = arguments.get("watch_id")

        if not watch_id:
            return [TextContent(type="text", text="错误: 缺少 watch_id 参数")]

        # 检查监控是否存在
        if watch_id not in self.watch_targets:
            return [TextContent(type="text", text=f"错误: 未找到监控 ID: {watch_id}")]

        # 权限检查
        target = self.watch_targets[watch_id]
        if target.process_id != self.process_id:
            return [TextContent(
                type="text",
                text=f"错误: 权限不足 - 该监控由进程 {target.process_id[:8]} 创建"
            )]

        # 移除监控（无论是active还是pending）
        removed_target = self.watch_targets.pop(watch_id)

        # 检查目录监控
        file_dir = os.path.dirname(os.path.abspath(removed_target.file_path))
        other_files_in_dir = any(
            os.path.dirname(os.path.abspath(t.file_path)) == file_dir
            for wid, t in self.watch_targets.items()
        )

        # 如果该目录下没有其他监控，取消目录监控
        if not other_files_in_dir and file_dir in self.directory_watchers:
            self.observer.unschedule(self.directory_watchers[file_dir])
            del self.directory_watchers[file_dir]
            logger.info(f"停止监控目录: {file_dir}")

        # 清理file_last_contents
        if removed_target.file_path in self.file_last_contents:
            del self.file_last_contents[removed_target.file_path]

        logger.info(f"取消监控: {watch_id}, 状态: {removed_target.status}, 文件: {removed_target.file_path}")

        status_msg = "延迟监控已取消" if removed_target.status == "pending" else "监控已取消"

        return [TextContent(
            type="text",
            text=f"{status_msg}: {watch_id}\n"
                 f"文件: {removed_target.file_path}\n"
                 f"状态: {removed_target.status}"
        )]

    async def _handle_list_watches(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """列出所有监控（包括延迟监控）"""
        show_pending = arguments.get("show_pending", True)

        watches = []
        active_count = 0
        pending_count = 0

        for watch_id, target in self.watch_targets.items():
            # 根据show_pending参数过滤
            if not show_pending and target.status == "pending":
                continue

            watches.append({
                "watch_id": watch_id,
                "file_path": target.file_path,
                "callback_tool": target.callback_tool,
                "process_id": target.process_id[:8],
                "status": target.status,
                "enabled": target.enabled,
                "encoding": target.encoding,
                "file_exists": os.path.isfile(target.file_path)
            })

            if target.status == "active":
                active_count += 1
            elif target.status == "pending":
                pending_count += 1

        summary = {
            "total_watches": len(watches),
            "active_watches": active_count,
            "pending_watches": pending_count,
            "directory_watchers": len(self.directory_watchers),
            "show_pending": show_pending
        }

        return [TextContent(
            type="text",
            text=f"监控列表:\n{json.dumps(summary, ensure_ascii=False, indent=2)}\n\n"
                 f"详细列表:\n{json.dumps(watches, ensure_ascii=False, indent=2)}"
        )]

    async def _handle_get_file_content(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """获取文件内容"""
        file_path = arguments.get("file_path")
        encoding = arguments.get("encoding", "utf-8")

        if not file_path:
            return [TextContent(type="text", text="错误: 未指定文件路径")]

        if not os.path.isfile(file_path):
            return [TextContent(type="text", text=f"错误: 文件不存在: {file_path}")]

        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()

            result = {
                "file_path": file_path,
                "content": content,
                "length": len(content)
            }

            return [TextContent(
                type="text",
                text=f"文件内容:\n{json.dumps(result, ensure_ascii=False, indent=2)}"
            )]
        except Exception as e:
            return [TextContent(type="text", text=f"错误: 无法读取文件: {str(e)}")]

    async def _handle_stop_watcher(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """停止监控器"""
        # 清理所有监控
        self.watch_targets.clear()

        # 停止Observer
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
            self.event_handler = None
            self.directory_watchers.clear()

        # 清理状态
        self.file_last_contents.clear()

        logger.info(f"文件监控器已停止，进程: {self.process_id[:8]}")

        return [TextContent(
            type="text",
            text=f"文件监控器已停止，所有监控已清除\n进程: {self.process_id[:8]}"
        )]

    async def _handle_get_stats(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """获取统计信息"""
        active_count = sum(1 for t in self.watch_targets.values() if t.status == "active")
        pending_count = sum(1 for t in self.watch_targets.values() if t.status == "pending")

        # 事件队列统计
        event_stats = self.event_queue.get_stats()

        stats = {
            "process_id": self.process_id[:8],
            "total_watches": len(self.watch_targets),
            "active_watches": active_count,
            "pending_watches": pending_count,
            "directory_watch_count": len(self.directory_watchers),
            "observer_running": self.observer is not None and self.observer.is_alive(),
            "event_queue": event_stats,
            "features": {
                "delayed_watch_support": True,
                "multi_session_support": True,
                "batch_processing": True,
                "auto_activation": True,
                "event_queue_support": True
            }
        }

        return [TextContent(
            type="text",
            text=f"监控服务器统计信息:\n{json.dumps(stats, ensure_ascii=False, indent=2)}"
        )]

    async def _handle_check_events(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """
        检查事件队列

        返回未被处理的文件变化事件。
        Agent 可以定期调用此工具获取事件，然后执行相应的 callback_tool。
        """
        limit = arguments.get("limit", 10)
        mark_retrieved = arguments.get("mark_retrieved", True)
        event_types = arguments.get("event_types", None)

        # 获取未检索的事件
        events = self.event_queue.get_unretrieved(limit, event_types)

        if not events:
            return [TextContent(
                type="text",
                text="事件队列中暂无新事件。\n建议：定期调用此工具（如每5-10秒）以获取文件变化通知。"
            )]

        # 构建返回结果
        event_list = []
        for event in events:
            event_data = {
                "event_id": event.event_id,
                "watch_id": event.watch_id,
                "file_path": event.file_path,
                "event_type": event.event_type,
                "content_length": event.content_length,
                "callback_tool": event.callback_tool,
                "process_id": event.process_id,
                "timestamp": event.timestamp,
                # 注意：内容可能很长，默认不返回完整内容
                # Agent 可通过 get_file_content 获取完整内容
            }
            event_list.append(event_data)

        # 标记为已检索
        if mark_retrieved:
            event_ids = [e.event_id for e in events]
            self.event_queue.mark_retrieved(event_ids)

        result = {
            "total_events": len(event_list),
            "mark_retrieved": mark_retrieved,
            "message": f"返回 {len(event_list)} 个事件。"
                       f"{'已标记为检索，下次调用不再返回。' if mark_retrieved else '未标记，下次调用仍会返回。'}",
            "events": event_list,
            "tip": "建议对每个事件执行 callback_tool 进行处理。内容可通过 get_file_content 获取。"
        }

        logger.info(f"check_events: 返回 {len(event_list)} 个事件, mark_retrieved={mark_retrieved}")

        return [TextContent(
            type="text",
            text=f"文件变化事件:\n{json.dumps(result, ensure_ascii=False, indent=2)}"
        )]

    async def _handle_cleanup_events(self, arguments: Dict[str, Any]) -> List[TextContent]:
        """清理过期事件"""
        cleaned_count = self.event_queue.cleanup_old_events()

        result = {
            "cleaned_count": cleaned_count,
            "remaining_events": len(self.event_queue.events),
            "message": f"清理了 {cleaned_count} 个过期事件（超过1小时的旧事件）。"
        }

        logger.info(f"cleanup_events: 清理了 {cleaned_count} 个过期事件")

        return [TextContent(
            type="text",
            text=f"事件清理结果:\n{json.dumps(result, ensure_ascii=False, indent=2)}"
        )]

    async def _handle_file_update_batch(self, file_path: str,
                                         matched_targets: List[Tuple[str, WatchTarget]]):
        """批量处理文件更新"""
        logger.info(f"批量处理文件更新: {file_path}, 监控数: {len(matched_targets)}")

        # 过滤已启用的active监控
        enabled_targets = [(wid, tgt) for wid, tgt in matched_targets
                          if tgt.enabled and tgt.status == "active"]

        if not enabled_targets:
            logger.info("所有监控已禁用或未激活，跳过处理")
            return

        await self._process_file_update(file_path, enabled_targets)

    async def _process_file_update(self, file_path: str,
                                   enabled_targets: List[Tuple[str, WatchTarget]]):
        """处理文件更新"""
        try:
            await asyncio.sleep(0.1)

            encoding = enabled_targets[0][1].encoding

            # 只读取一次文件
            with open(file_path, 'r', encoding=encoding) as f:
                new_content = f.read()

            logger.info(f"文件内容已读取，长度: {len(new_content)}")

            # 检查内容是否变化
            last_content = self.file_last_contents.get(file_path, "")

            if new_content == last_content:
                logger.info("文件内容未实际变化，跳过所有回调")
                return

            # 更新共享的last_content
            self.file_last_contents[file_path] = new_content

            # 更新所有target的last_content
            for watch_id, target in enabled_targets:
                target.last_content = new_content

            logger.info(f"文件内容已更新，准备触发 {len(enabled_targets)} 个回调")

            # 并发触发所有回调
            callback_tasks = []
            for watch_id, target in enabled_targets:
                callback_tasks.append(
                    self._invoke_callback(
                        watch_id,
                        target.callback_tool,
                        file_path,
                        new_content,
                        target.process_id,
                        event_type="file_updated"
                    )
                )

            if callback_tasks:
                results = await asyncio.gather(*callback_tasks, return_exceptions=True)

                success_count = sum(1 for r in results if not isinstance(r, Exception))
                error_count = sum(1 for r in results if isinstance(r, Exception))

                logger.info(f"回调执行完成: 成功 {success_count}, 失败 {error_count}")

        except Exception as e:
            logger.error(f"批量处理文件更新错误: {file_path}", exc_info=True)

    async def _invoke_callback(self, watch_id: str, tool_name: str,
                               file_path: str, content: str,
                               process_id: str,
                               event_type: str = "file_updated") -> Dict[str, Any]:
        """
        触发回调

        流程：
        1. 创建事件并存入队列（确保不丢失）
        2. 返回事件信息

        Agent 通过调用 check_events 工具轮询获取事件。
        """
        import time

        # 创建事件并存入队列
        event_id = self.event_queue.generate_event_id(process_id)
        event = FileEvent(
            event_id=event_id,
            watch_id=watch_id,
            file_path=file_path,
            event_type=event_type,
            content=content,
            content_length=len(content),
            callback_tool=tool_name,
            process_id=process_id[:8],
            timestamp=time.time(),
            retrieved=False
        )

        # 存入队列（确保不丢失）
        self.event_queue.add(event)
        logger.info(f"事件已存入队列: {event_id}, 类型: {event_type}")

        # 构建回调信息
        callback_info = {
            "event": event_type,
            "event_id": event_id,
            "watch_id": watch_id,
            "file_path": file_path,
            "content": content,
            "content_length": len(content),
            "callback_tool": tool_name,
            "process_id": process_id[:8],
            "timestamp": str(time.time()),
            "queue_status": {
                "total_events": len(self.event_queue.events),
                "unretrieved": self.event_queue.get_stats()["unretrieved_events"]
            },
            "tip": "请定期调用 check_events 工具获取文件变化事件"
        }

        logger.info(f"事件触发: watch_id={watch_id}, event={event_type}, "
                    f"tool={tool_name}, content_len={len(content)}, event_id={event_id}")

        return callback_info

    async def run(self):
        """运行MCP服务器"""
        logger.info(f"启动延迟监控MCP服务器: {self.name}, "
                    f"进程ID: {self.process_id[:8]}")

        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )

    async def cleanup(self):
        """清理资源"""
        logger.info(f"清理资源: {self.process_id[:8]}")
        await self._handle_stop_watcher({})


async def main():
    """主入口函数"""
    server = DelayedFileWatcherServer()

    try:
        await server.run()
    finally:
        await server.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
