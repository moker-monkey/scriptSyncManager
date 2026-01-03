#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
脚本同步管理器 - 命令行接口
用于管理脚本的执行和测试

使用方法:
    python manager.py run <script_name> [options]
    python manager.py test <script_name> [options]
    python manager.py ls [options]
    python manager.py --help
"""

import sys
import argparse
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.append("/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager")

from core.handler import ScriptHandler
from core.config import config


class Manager:
    """
    脚本管理器 - 提供命令行接口功能
    """

    def __init__(self):
        """初始化管理器"""
        self.handler = ScriptHandler()

    def run(
        self,
        script_name: str,
        func_name: Optional[str] = None,
        save_to_db: bool = True,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        运行指定脚本

        Args:
            script_name (str): 脚本名称（不含.py扩展名）
            func_name (Optional[str]): 要执行的函数名称，默认为main或run
            save_to_db (bool): 是否保存结果到数据库
            verbose (bool): 是否显示详细输出

        Returns:
            Dict[str, Any]: 执行结果
        """
        print(f"🚀 开始执行脚本: {script_name}")
        if func_name:
            print(f"   目标函数: {func_name}")
        print(f"   数据库存储: {'启用' if save_to_db else '禁用'}")
        print(f"   执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        try:
            result = self.handler.execute_script(
                script_name=script_name, func_name=func_name, save_to_db=save_to_db
            )

            # 显示执行结果
            self._print_execution_result(result, verbose)

            return result

        except Exception as e:
            error_result = {
                "success": False,
                "script_name": script_name,
                "execution_time": datetime.now(),
                "result": None,
                "message": f"执行失败: {str(e)}",
                "data_stored": False,
            }

            print(f"❌ 执行失败: {str(e)}")
            return error_result

    def list(self, filter_type: str = "all", verbose: bool = False) -> Dict[str, Any]:
        """
        列出可用的脚本

        Args:
            filter_type (str): 过滤类型 ('all', 'regular', 'test')
            verbose (bool): 是否显示详细信息

        Returns:
            Dict[str, Any]: 脚本列表信息
        """
        print("📋 脚本列表")
        print("-" * 50)

        try:
            scripts_info = self.handler.list_available_scripts()

            if filter_type == "all":
                display_scripts = (
                    scripts_info["regular_scripts"] + scripts_info["test_scripts"]
                )
                title = "所有脚本"
            elif filter_type == "regular":
                display_scripts = scripts_info["regular_scripts"]
                title = "常规脚本"
            elif filter_type == "test":
                display_scripts = scripts_info["test_scripts"]
                title = "测试脚本"
            else:
                display_scripts = []
                title = "未知类型"

            print(f"{title} (共 {len(display_scripts)} 个):")
            print()

            for i, script in enumerate(display_scripts, 1):
                script_type = "🧪 测试" if script["is_test"] else "🚀 常规"
                print(f"{i:2d}. {script_type} {script['name']}")

                if verbose:
                    print(f"    文件: {script['file_path']}")
                    print(
                        f"    修改时间: {script['modified_time'].strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    print()

            if not display_scripts:
                print("   未找到匹配的脚本")

            return scripts_info

        except Exception as e:
            print(f"❌ 获取脚本列表失败: {str(e)}")
            return {"total": 0, "regular_scripts": [], "test_scripts": []}

    def _print_execution_result(
        self, result: Dict[str, Any], verbose: bool = False
    ) -> None:
        """
        打印执行结果

        Args:
            result (Dict[str, Any]): 执行结果
            verbose (bool): 是否显示详细信息
        """
        success_icon = "✅" if result["success"] else "❌"
        print(f"{success_icon} 执行结果:")
        print(f"   脚本: {result['script_name']}")
        print(f"   时间: {result['execution_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   状态: {'成功' if result['success'] else '失败'}")
        print(f"   消息: {result['message']}")

        if result["data_stored"]:
            print("   数据库: ✅ 已存储")
        else:
            print("   数据库: ❌ 未存储")

        if verbose and result["result"] is not None:
            print("   结果详情:")
            if (
                hasattr(result["result"], "__len__")
                and len(str(result["result"])) > 200
            ):
                # 对于较长的结果，显示前200个字符
                result_str = str(result["result"])[:200] + "..."
                print(f"   {result_str}")
            else:
                print(f"   {result['result']}")

        print("-" * 50)

    def convert_menu(
        self, menu_path: str = None, verbose: bool = False
    ) -> Dict[str, Any]:
        """
        将 Menu.json 转换为脚本调度配置并更新数据库

        Args:
            menu_path (str): Menu.json 文件路径，如果为 None 则使用项目根目录的 Menu.json
            verbose (bool): 是否显示详细信息

        Returns:
            Dict[str, Any]: 转换和更新结果
        """
        print("🔄 开始转换 Menu.json")
        if menu_path:
            print(f"   指定路径: {menu_path}")
        else:
            print("   使用默认路径: Menu.json")
        print(f"   执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        try:
            result = self.handler.convert_menu(menu_path)

            # 显示转换结果
            self._print_convert_result(result, verbose)

            return result

        except Exception as e:
            error_result = {
                "success": False,
                "menu_path": menu_path or "默认路径",
                "total_items": 0,
                "created_items": 0,
                "updated_items": 0,
                "skipped_items": 0,
                "message": f"转换失败: {str(e)}",
                "details": [],
            }

            print(f"❌ 转换失败: {str(e)}")
            return error_result

    def _print_convert_result(
        self, result: Dict[str, Any], verbose: bool = False
    ) -> None:
        """
        打印转换结果

        Args:
            result (Dict[str, Any]): 转换结果
            verbose (bool): 是否显示详细信息
        """
        success_icon = "✅" if result["success"] else "❌"
        print(f"{success_icon} 转换结果:")
        print(f"   Menu文件: {result['menu_path']}")
        print(f"   总条目数: {result['total_items']}")
        print(f"   新创建: {result['created_items']}")
        print(f"   更新: {result['updated_items']}")
        print(f"   跳过: {result['skipped_items']}")
        print(f"   状态: {'成功' if result['success'] else '失败'}")
        print(f"   消息: {result['message']}")

        if verbose and result["details"]:
            print("   详细处理结果:")
            for detail in result["details"]:
                action_icon = "🆕" if detail["action"] == "created" else "🔄"
                preserved = (
                    " (保留last_sync_datetime)"
                    if detail.get("last_sync_preserved")
                    else ""
                )
                print(
                    f"   {action_icon} {detail['script_name']} - {detail['action']}{preserved}"
                )
                print(f"      周期: {detail['period']}, 启用: {detail['turn_on']}")

        print("-" * 50)


def create_parser() -> argparse.ArgumentParser:
    """
    创建命令行参数解析器

    Returns:
        argparse.ArgumentParser: 参数解析器
    """
    parser = argparse.ArgumentParser(
        description="脚本同步管理器 - 用于执行和管理脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s run sample_data_script                    # 运行脚本并保存到数据库，默认执行init函数
  %(prog)s run sample_data_script --func main       # 运行指定函数
  %(prog)s run sample_data_script --no-db           # 不保存到数据库
  %(prog)s ls                                    # 列出所有脚本
  %(prog)s ls --filter test                      # 只显示测试脚本
  %(prog)s convert-menu                             # 转换 Menu.json
  %(prog)s convert-menu --menu-path path/to/Menu.json  # 指定 Menu.json 路径
  %(prog)s convert-menu -v                         # 转换 Menu.json 并显示详细信息
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # run 命令
    run_parser = subparsers.add_parser("run", help="运行指定脚本")
    run_parser.add_argument("script_name", help="脚本名称（不含.py扩展名）")
    run_parser.add_argument(
        "--func", dest="func_name", help="要执行的函数名称（默认为main或run）"
    )
    run_parser.add_argument("--no-db", action="store_true", help="不保存结果到数据库")
    run_parser.add_argument("-v", "--verbose", action="store_true", help="显示详细信息")

    # list 命令
    list_parser = subparsers.add_parser("ls", help="列出可用的脚本")
    list_parser.add_argument(
        "--filter",
        choices=["all", "regular", "test"],
        default="all",
        help="脚本过滤类型",
    )
    list_parser.add_argument(
        "-v", "--verbose", action="store_true", help="显示详细信息"
    )

    # convert-menu 命令
    convert_menu_parser = subparsers.add_parser(
        "convert-menu", help="将 Menu.json 转换为脚本调度配置"
    )
    convert_menu_parser.add_argument(
        "--menu-path", help="Menu.json 文件路径（默认为项目根目录的 Menu.json）"
    )
    convert_menu_parser.add_argument(
        "-v", "--verbose", action="store_true", help="显示详细信息"
    )

    return parser


def main():
    """
    主函数 - 命令行入口点
    """
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # 创建管理器实例
    manager = Manager()

    try:
        if args.command == "run":
            result = manager.run(
                script_name=args.script_name,
                func_name=args.func_name or "init",
                save_to_db=not args.no_db,
                verbose=args.verbose,
            )
            # 根据执行结果设置退出码
            sys.exit(0 if result["success"] else 1)

        elif args.command == "ls":
            result = manager.list(filter_type=args.filter, verbose=args.verbose)
            sys.exit(0)

        elif args.command == "convert-menu":
            result = manager.convert_menu(
                menu_path=args.menu_path, verbose=args.verbose
            )
            # 根据转换结果设置退出码
            sys.exit(0 if result["success"] else 1)

    except KeyboardInterrupt:
        print("\n\n⚠️  操作被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生未预期的错误: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
