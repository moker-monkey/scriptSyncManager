import os
import sys
import importlib.util
import inspect
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlmodel import Session, SQLModel
import logging
import json
from croniter import croniter
import random
import time


from .config import config
from .models import ScriptSyncMenu
from .tools import calculate_next_sync_time, import_script, store_dataframe_to_db, save_result_to_json, load_result_from_json, get_script_result, has_saved_result, get_or_create_script_schedule, store_execution_result


class ScriptHandler:
    """
    脚本执行处理器
    负责执行指定脚本，处理参数，管理数据存储
    """

    def __init__(self):
        """初始化脚本处理器"""
        self.scripts_dir = Path(config.base_dir) / "scripts"
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        设置日志记录器

        Returns:
            logging.Logger: 配置好的日志记录器
        """
        logger = logging.getLogger("script_handler")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def retry_script(self, script_name: str) -> Dict[str, Any]:
        """
        重试脚本的指定函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）

        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            # 1. 先读取result JSON的数据
            result = get_script_result(script_name, self.logger)
            if not result:
                self.logger.error(f"无法读取脚本 {script_name} 的历史执行数据")
                return {
                    "success": False,
                    "message": f"无法读取脚本 {script_name} 的历史执行数据"
                }

            if result.get("success_count") != result.get("total_count"):  # 中途中断情况
                self.logger.info(f"脚本 {script_name} 上次执行中途中断，成功执行 {result.get('success_count')} 个，总共 {result.get('total_count')} 个")
            
            # 5. 执行脚本
            func_name = result.get("func_name")
            self.logger.info(f"开始重试脚本 {script_name}，函数: {func_name}")
            result = self._execute_script(
                script_name=script_name,
                func_name=func_name,
                type="iterator",
                script_config=result.get("script_config"),
                is_exists=result.get("is_exists"),
                save_to_db=result.get("save_to_db"),
                start_index=result.get("success_count"),
                total_count=result.get("total_count"),
                error_items=result.get("error_items"),
                errors=result.get("errors"),
                error_start_index=result.get("error_start_index"),
            )
            
            self.logger.info(f"脚本 {script_name} 重试完成，执行结果: {'成功' if result.get('success') else '失败'}")
            return result
            
        except Exception as e:
            self.logger.error(f"重试脚本 {script_name} 失败: {str(e)}")
            return {
                "success": False,
                "message": f"重试脚本 {script_name} 失败: {str(e)}"
            }

    def _execute_script(
        self,
        script_name: Any,
        func_name: str,
        save_to_db: bool = True,
        type: Optional[Union['iterator', 'single']] = "single",
        is_exists: str = "append",
        depend_result: Optional[Dict[str, Any]] = None,
        script_config: Optional[Dict[str, Any]] = None,
        start_index: int = 0,
        total_count: int = 0,
        error_items: Optional[List[Dict[str, Any]]] = [],
        errors: Optional[List[str]] = [],
        error_start_index: int = 0
    ) -> Dict[str, Any]:
        """
        执行脚本的指定函数

        Args:
            script_name (str): 导入的脚本模块
            func_name (str): 要执行的函数名称
            save_to_db (bool): 是否将结果存储到数据库
            is_exists (str): 表存在时的处理方式，默认是append
            depend_result (Optional[Dict[str, Any]]): 依赖函数的执行结果
            type (Optional[Union['iterator', 'single']]): 执行类型，默认是single
            script_config (Optional[Dict[str, Any]]): 脚本配置（遍历时两次之间的时间间隔）
            start_index (int): 开始索引，默认是0
            total_count (int): 总数量，默认是0
            error_count (int): 错误数量，默认是0
            error_start_index (int): 错误开始索引，默认是0,用于错误信息中断记录

        Returns:
            Dict[str, Any]: 执行结果
        """
        if script_config is None:
                script_config = {
                    "interval": 4,
                    "is_error_stop": True,
                }

        result = {
            "success": False,
            "script_name": script_name,
            "func_name": func_name,
            "save_to_db": save_to_db,
            "type": type,
            "script_config": script_config,
            "is_exists": is_exists,
            "total_count": total_count,
            "success_count": start_index,
            "execution_time": datetime.now(),
            "finish_time": None,
            "error_items": error_items.copy() if isinstance(error_items, list) else [],
            "errors": errors.copy() if isinstance(errors, list) else [],
            "error_start_index": error_start_index,
        }

        # 确保 actual_script_name 始终被初始化
        actual_script_name = str(script_name)
        try:
            # 初始化数据库引擎
            engines = config.init_db()
            script_engine = engines["script_engine"]
            
            # 调用脚本模块的指定函数
            module = import_script(script_name, self.scripts_dir, self.logger)
            func = getattr(module, func_name)
            script_schedule = get_or_create_script_schedule(script_name, self.logger)
            
            depend_func = None
            
            if hasattr(module, 'depend'):
                depend_func = getattr(module, 'depend')
                if depend_result is None:
                    depend_result = depend_func(script_schedule, self)
            # 处理依赖结果
            if depend_result is None:
                depend_result = []
            
            result["total_count"] = total_count or len(depend_result)
            
            if type == "iterator":
                error_items = result.get("error_items")
                # 此处的bug为，既要恢复错误项，又要处理中断，两者会有重叠的部分
                # 处理中断是从success_count开始的,但是报错的是success_count+1
                # 因此，当我处理错误时会将success_count+1也处理，再从中断处继续执行就导致了重复执行
                # 为此，如果is_error_stop为False，我就不处理错误项，只处理中断项
                if not script_config.get("is_error_stop") and len(error_items):
                    for index, error_item in enumerate(error_items):
                        # 如果存在error_items，会先处理错误项
                        if result["error_start_index"] >= index:
                            self.logger.info(f"脚本 {script_name} 的函数 {func_name} 恢复第 {index} 个错误: {error_item}")
                            try:
                                func_result = func(script_schedule, self, error_item)
                                if save_to_db and func_result is not None:
                                    store_dataframe_to_db(func_result, table_name=script_name, engine=script_engine, logger=self.logger, is_exists="append")
                                result["error_start_index"] = index
                            except Exception as e:
                                self.logger.error(f"脚本 {script_name} 的函数 {func_name} 恢复第 {index} 个错误失败: {str(e)}")
                                result["error_items"][index] = error_item
                                result["errors"][index] = str(e)
                                result["error_start_index"] = index
                                print(f'二次报错，错误索引为 {index}, {result}')
                                break
                    if result["error_start_index"] >= len(error_items):
                        # 如果所有错误项都处理成功，清空错误项和错误列表
                        result["error_items"] = []
                        result["errors"] = []
                        result["error_start_index"] = 0
                    else:
                        result["success"] = False
                        result["message"] = f"错误恢复失败，错误索引为 {result['error_start_index']}， 错误原因: {result['errors'][result['error_start_index']]}，请检查错误项并重试"
                        raise Exception(result["message"])

                if not isinstance(depend_result, list):
                    self.logger.error(f"脚本 {script_name} 的 depend 函数返回的结果不是列表")
                    result["success"] = False
                    result["message"] = "depend 函数返回的结果不是列表"
                    return result
                depend_result = depend_result[result["success_count"]:]
                if script_config.get("is_error_stop"):
                    result["error_start_index"] = []
                    result["error_items"] = []
                    result["error_start_index"] = 0

                if len(depend_result) == 0:
                    result["success"] = True
                    result["message"] = "执行成功,没有更多数据"
                    return result
                else:
                    print(f'从当前成功次数为 {result["success_count"]} 开始执行')

                for item in depend_result:
                    self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行第 {result['success_count']} 次")
                    try:
                        func_result = func(script_schedule, self, item)
                        if func_result is None:
                            self.logger.error(f"脚本 {script_name} 的函数 {func_name} 执行第 {result['success_count']} 次返回 None")
                        else:
                            if save_to_db:
                                store_dataframe_to_db(func_result, table_name=script_name, engine=script_engine, logger=self.logger, is_exists="append")
                            self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行第 {result['success_count']} 次结果: {func_result}")
                        
                    except Exception as e:
                        self.logger.error(f"脚本 {script_name} 的函数 {func_name} 执行第 {result['success_count']} 次失败: {str(e)}")
                        result["error_items"].append(item)
                        result["errors"].append(str(e))
                        if script_config["is_error_stop"]:
                            break

                    result["success_count"] += 1
                    interval_val = script_config["interval"]
                    if isinstance(interval_val, str) and "-" in interval_val:
                        start, end = map(int, interval_val.split("-"))
                        sleep_time = random.randint(start, end)
                    else:
                        sleep_time = int(interval_val)
                    time.sleep(sleep_time)
                result["success"] = True
                result["message"] = "执行成功"
            else:
                # 执行单例函数
                func_result = func(script_schedule, self, depend_result)
                self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行结果: {func_result}")
                # 如果需要，将结果存储到数据库
                if save_to_db and func_result is not None:
                    store_dataframe_to_db(func_result, table_name=script_name, engine=script_engine, logger=self.logger, is_exists=is_exists)
                    # 保存最终结果到JSON文件
                result["success"] = True
                result["message"] = "执行成功"

        except Exception as e:
            self.logger.error(f"执行脚本 {script_name} 的函数 {func_name} 失败: {str(e)}")
            result["success"] = False
            result["message"] = f"执行失败: {str(e)}"
            
        finally:
            result["finish_time"] = datetime.now()
            save_result_to_json(actual_script_name, result, self.logger)
            return result

    def list_available_scripts(self) -> Dict[str, Any]:
        """
        列出所有可用的脚本

        Returns:
            Dict[str, Any]: 脚本列表信息
        """
        scripts_info = {"total": 0, "regular_scripts": [], "test_scripts": []}

        if not self.scripts_dir.exists():
            self.logger.warning(f"脚本目录不存在: {self.scripts_dir}")
            return scripts_info

        python_files = list(self.scripts_dir.glob("*.py"))

        for script_file in python_files:
            script_name = script_file.stem

            # 跳过__init__.py等特殊文件
            if script_name.startswith("__"):
                continue


            script_info = {
                "name": script_name,
                "file_path": str(script_file),
                "modified_time": datetime.fromtimestamp(script_file.stat().st_mtime),
            }

            scripts_info["regular_scripts"].append(script_info)

        scripts_info["total"] = len(scripts_info["regular_scripts"]) + len(
            scripts_info["test_scripts"]
        )

        return scripts_info