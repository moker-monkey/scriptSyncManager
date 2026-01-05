#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
脚本同步管理器 - 命令行接口
用于管理脚本的执行和测试

使用方法:
    python manager.py run <script_name> [options]  运行指定脚本
    python manager.py retry <script_name> [options]  重试指定脚本
    python manager.py test <script_name> [options]  测试指定脚本
    python manager.py ls [options]  列出所有脚本
    python manager.py pf <script_name> [options]  列出脚本中的所有函数
    python manager.py --help  显示帮助信息
"""

import sys
import argparse
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

    def run_init(self,script_name: str) -> Dict[str, Any]:
        """
        运行指定脚本的init函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）

        Returns:
            Dict[str, Any]: 执行结果
        """
        return self.handler._execute_script(script_name, 'init', type="single", is_exists="replace")

    def run_iteration(self,script_name: str, interval: str = "1-5", is_error_stop: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        """
        运行指定脚本的iteration函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）
            interval (str): 遍历时两次之间的时间间隔，支持范围值（如"1-5"）或固定值
            is_error_stop (bool): 执行出错时是否停止，默认为True
            save_to_db (bool): 是否保存结果到数据库

        Returns:
            Dict[str, Any]: 执行结果
        """
        # 使用默认配置字典，而不是Config对象
        print(f"interval: {interval}, is_error_stop: {is_error_stop}, save_to_db: {save_to_db}")
        script_config = {
            "interval": interval,
            "is_error_stop": is_error_stop,
        }
        return self.handler._execute_script(
            script_name=script_name,
            func_name='iteration',
            type="iterator",
            script_config=script_config,
            save_to_db=save_to_db
        )

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
            func_name (Optional[str]): 要执行的函数名称，默认为main或run，最后尝试init
            save_to_db (bool): 是否保存结果到数据库
            type (str): 执行类型，single或iterator，默认single
            is_exists (str): 数据库中存在数据时的处理方式，replace或append，默认replace
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

        # 执行脚本，支持多种函数名
        try:
            # 如果没有指定函数名，尝试使用main或run，最后尝试init
            if func_name is None:
                # 如果没有找到任何候选函数，默认使用init
                func_name = 'init'
            
            # 执行脚本
            result = self.handler._execute_script(
                script_name=script_name,
                func_name=func_name,
                save_to_db=save_to_db,
                type="single"
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

            print(f"{title} (共 {len(display_scripts)} 个):")
            print()

            for i, script in enumerate(display_scripts, 1):
                script_type = "🚀"
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


        if verbose and "result" in result and result["result"] is not None:
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
        self,
        menu_path: str = None,
        verbose: bool = False
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
            result = config.convert_menu(menu_path)

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

    def retry(
        self,
        script_name: str,
        verbose: bool = False
    ) -> Dict[str, Any]:
        """
        重试指定脚本

        Args:
            script_name (str): 脚本名称（不含.py扩展名）
            verbose (bool): 是否显示详细信息

        Returns:
            Dict[str, Any]: 执行结果
        """
        print(f"🔄 开始重试脚本: {script_name}")
        print(f"   执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        try:
            result = self.handler.retry_script(script_name)

            # 显示执行结果
            self._print_execution_result(result, verbose)

            return result

        except Exception as e:
            error_result = {
                "success": False,
                "script_name": script_name,
                "execution_time": datetime.now(),
                "result": None,
                "message": f"重试失败: {str(e)}",
            }

            print(f"❌ 重试失败: {str(e)}")
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

def print_func(script_name: str) -> None:
    """
    打印脚本下的全部函数名称

    Args:
        script_name (str): 脚本名称
    """
    import inspect
    import logging
    from pathlib import Path
    from core.tools import import_script
    from core.config import config
    
    # 设置日志记录器
    logger = logging.getLogger("print_func")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    print(f"脚本名称: {script_name}")
    print("=" * 50)
    
    try:
        # 构建脚本目录路径
        scripts_dir = Path(config.base_dir) / "scripts"
        
        # 导入脚本模块
        module = import_script(script_name, scripts_dir, logger)
        
        # 获取脚本中的所有函数
        functions = []
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            # 过滤掉内置函数和私有函数（以下划线开头的函数）
            if not name.startswith("_"):
                # 获取函数签名
                sig = inspect.signature(obj)
                functions.append((name, sig))
        
        if not functions:
            print("  该脚本中没有可执行的函数")
        else:
            print(f"  共找到 {len(functions)} 个函数:")
            print("  " + "-" * 46)
            for func_name, func_sig in functions:
                # 检查是否是脚本的基本函数（init, period, depend, iteration）
                is_basic_func = func_name in ["init", "period", "depend", "iteration"]
                func_type = "[基础函数]" if is_basic_func else "[辅助函数]"
                print(f"  {func_type} {func_name}{func_sig}")
    
    except FileNotFoundError:
        print(f"  错误: 脚本 '{script_name}' 不存在")
    except ImportError as e:
        print(f"  错误: 导入脚本失败 - {str(e)}")
    except Exception as e:
        print(f"  错误: 处理脚本时发生异常 - {str(e)}")
    
    print("=" * 50)

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
        "--func", dest="func_name", help="要执行的函数名称（不写默认为init）"
    )
    run_parser.add_argument("--no-db", action="store_false", dest="save_to_db", help="不保存结果到数据库")
    run_parser.add_argument("--init", action="store_true", help="执行init函数")
    run_parser.add_argument("--iterator", action="store_true", help="执行iterator函数")
    run_parser.add_argument("--interval", help="遍历时两次之间的时间间隔，支持范围值（如\"1-5\"）或固定值")
    run_parser.add_argument("--no-error-stop", action="store_false", dest="is_error_stop", help="执行出错时不停止")
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
    convert_menu_parser = subparsers.add_parser("convert-menu", help="将 Menu.json 转换为脚本调度配置")
    convert_menu_parser.add_argument(
        "--menu-path", help="Menu.json 文件路径（默认为项目根目录的 Menu.json）"
    )
    convert_menu_parser.add_argument(
        "-v", "--verbose", action="store_true", help="显示详细信息"
    )
    
    # retry 命令
    retry_parser = subparsers.add_parser("retry", help="重试指定脚本")
    retry_parser.add_argument("script_name", help="脚本名称（不含.py扩展名）")
    retry_parser.add_argument(
        "-v", "--verbose", action="store_true", help="显示详细信息"
    )
    
    # print-func 命令
    print_func_parser = subparsers.add_parser("pf", help="打印指定脚本的所有函数")
    print_func_parser.add_argument("script_name", help="脚本名称（不含.py扩展名）")

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
            # 根据是否执行init函数和iterator函数来调用不同的方法
            if args.init:
                result = manager.run_init(script_name=args.script_name)
            elif args.iterator:
                result = manager.run_iteration(
                    script_name=args.script_name,
                    interval=args.interval if args.interval else "1-5",
                    is_error_stop=args.is_error_stop,
                    save_to_db=args.save_to_db
                )
            else:
                result = manager.run(
                    script_name=args.script_name,
                    func_name=args.func_name,
                    save_to_db=args.save_to_db,
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
        
        elif args.command == "retry":
            result = manager.retry(
                script_name=args.script_name, verbose=args.verbose
            )
            # 根据执行结果设置退出码
            sys.exit(0 if result["success"] else 1)
        
        elif args.command == "pf":
            print_func(args.script_name)
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n⚠️  操作被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生未预期的错误: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
