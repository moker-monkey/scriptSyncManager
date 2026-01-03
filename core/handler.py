import os
import sys
import importlib.util
import inspect
from typing import Any, Dict, Optional, Union
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
from .models import ScriptSchedule


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

    def _calculate_next_sync_time(
        self, last_sync_time: datetime, cron_expression: str
    ) -> Optional[datetime]:
        """
        根据上次执行时间和crontab表达式计算下次执行时间

        Args:
            last_sync_time (datetime): 上次执行时间
            cron_expression (str): crontab表达式

        Returns:
            Optional[datetime]: 下次执行时间，计算失败返回None
        """
        if not cron_expression:
            self.logger.error("crontab表达式为空")
            return None
        try:
            # 使用croniter解析表达式并计算下次执行时间
            cron = croniter(cron_expression, last_sync_time)
            next_time = cron.get_next(datetime)
            return next_time
        except Exception as e:
            self.logger.error(f"计算下次执行时间失败: {str(e)}")
            return None

    def _import_script(self, script_name: str) -> Any:
        """
        动态导入脚本模块

        Args:
            script_name (str): 脚本名称（不含.py扩展名）

        Returns:
            Any: 导入的模块

        Raises:
            ImportError: 脚本文件不存在或导入失败
        """
        script_file = self.scripts_dir / f"{script_name}.py"

        if not script_file.exists():
            raise FileNotFoundError(f"脚本文件不存在: {script_file}")

        # 动态导入脚本
        spec = importlib.util.spec_from_file_location(script_name, script_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载脚本规范: {script_name}")

        module = importlib.util.module_from_spec(spec)

        # 添加脚本目录到Python路径
        if str(self.scripts_dir) not in sys.path:
            sys.path.insert(0, str(self.scripts_dir))

        try:
            spec.loader.exec_module(module)
            self.logger.info(f"成功导入脚本: {script_name}")
            return module
        except Exception as e:
            self.logger.error(f"导入脚本失败 {script_name}: {str(e)}")
            raise ImportError(f"导入脚本失败 {script_name}: {str(e)}")

    def _store_dataframe_to_db(self, df: pd.DataFrame, table_name: str, engine, is_exists: str = "append") -> bool:
        """
        将DataFrame存储到数据库

        Args:
            df (pd.DataFrame): 要存储的数据
            table_name (str): 表名
            engine: 数据库引擎
            is_exists (str): 表存在时的处理方式，默认是append

        Returns:
            bool: 是否存储成功
        """
        try:
            # 将DataFrame写入数据库，如果表已存在则替换
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=is_exists,
                index=False,
            )
            records = len(df)
            self.logger.info(f"成功存储 {records} 条记录到表 {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"存储DataFrame到数据库失败: {str(e)}")
            return False
    
    def test_script(self, script_name: str) -> Dict[str, Any]:
        """
        测试脚本的初始化函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）

        Returns:
            Dict[str, Any]: 执行结果
        """
        return self._execute_script(script_name, 'init', save_to_db=False, type="single", is_exists="replace")

    def run_init_script(self, script_name: str) -> Dict[str, Any]:
        """
        执行脚本的初始化函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）
            func_name (str): 要执行的初始化函数名称
            save_to_db (bool): 是否将结果存储到数据库

        Returns:
            Dict[str, Any]: 执行结果
        """
        return self._execute_script(script_name, 'init', type="single", is_exists="replace")

    def run_iterator_script(self, script_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行脚本的迭代函数

        Args:
            script_name (str): 脚本名称（不含.py扩展名）
            config (Dict[str, Any]): 迭代配置，包含interval（两次迭代之间的时间间隔）和is_error_stop（是否在遇到错误时停止）

        Returns:
            Dict[str, Any]: 执行结果
        """
        return self._execute_script(script_name, 'iteration', type="iterator", config=config)

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
            result = self.get_script_result(script_name)
            if not result:
                self.logger.error(f"无法读取脚本 {script_name} 的历史执行数据")
                return {
                    "success": False,
                    "message": f"无法读取脚本 {script_name} 的历史执行数据"
                }
            
            # 2. 从上一步的数据中提取参数
            func_name = result.get("func_name", "init")
            config = result.get("config", {})
            is_exists = result.get("is_exists", "append")
            save_to_db = result.get("save_to_db", True)
            error_items = result.get("error_items", [])
            
            # 3. 执行脚本
            self.logger.info(f"开始重试脚本 {script_name}，函数: {func_name}")
            result = self._execute_script(
                script_name=script_name,
                func_name=func_name,
                type="iterator",
                config=config,
                is_exists=is_exists,
                save_to_db=save_to_db,
                depend_result=error_items
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
        type: Optional['iterator' | 'single'] = "single",
        is_exists: str = "append",
        depend_result: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        执行脚本的指定函数

        Args:
            script_name (str): 导入的脚本模块
            func_name (str): 要执行的函数名称
            save_to_db (bool): 是否将结果存储到数据库
            is_exists (str): 表存在时的处理方式，默认是append
            depend_result (Optional[Dict[str, Any]]): 依赖函数的执行结果
            type (Optional['iterator' | 'single']): 执行类型，默认是single
            config (Optional[Dict[str, Any]]): 脚本配置（遍历时两次之间的时间间隔）

        Returns:
            Dict[str, Any]: 执行结果
        """
        if not config:
                config = {
                    "interval": 4,
                    "is_error_stop": True,
                }

        result = {
            "success": False,
            "script_name": script_name,
            "func_name": func_name,
            "total_count": 0,
            "error_count": 0,
            "execution_time": datetime.now(),
            "finish_time": None,
            "error_items": [],
            "errors": [],
            "type": type,
            "config": config,
            "is_exists": is_exists,
        }

        try:
            # 调用脚本模块的指定函数
            module = self._import_script(script_name)
            func = getattr(module, func_name)
            script_schedule = self._get_or_create_script_schedule(script_name)
            
            depend_func = None
            if hasattr(module, 'depend'):
                depend_func = getattr(module, 'depend')
                if depend_result is None:
                    depend_result = depend_func(script_schedule, self)
            result["total_count"] = len(depend_result)
            if type == "iterator":
                if not isinstance(depend_result, list):
                    self.logger.error(f"脚本 {script_name} 的 depend 函数返回的结果不是列表")
                    return {"success": False, "message": "depend 函数返回的结果不是列表"}
                for item in depend_result:
                    self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行第 {item} 次")
                    try:
                        result = func(script_schedule, self, item)
                        if result is None:
                            self.logger.error(f"脚本 {script_name} 的函数 {func_name} 执行第 {item} 次返回 None")
                            continue
                        
                        # 保存执行结果到JSON文件
                        actual_script_name = str(script_name.__name__) if hasattr(script_name, '__name__') else str(script_name)
                        
                        if save_to_db and result is not None:
                            self._store_dataframe_to_db(result, table_name=script_name, engine=self.engine, is_exists="append")

                        self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行第 {item} 次结果: {result}")
                        
                        
                    except Exception as e:
                        self.logger.error(result)
                        # 保存错误结果到JSON文件
                        print(f"脚本 {script_name} 的函数 {func_name} 执行第 {item} 次失败: {str(e)}")
                        result["error_items"].append(item)
                        result["errors"].append(str(e))
                        if config["is_error_stop"]:
                            break
                    time.sleep(random.randint(1, config["interval"]))
                    
            else:
                # 执行单例函数
                result = func(script_schedule, self, depend_result)
                self.logger.info(f"脚本 {script_name} 的函数 {func_name} 执行结果: {result}")

                # 如果需要，将结果存储到数据库
                if save_to_db and result is not None:
                    self._store_dataframe_to_db(result, table_name=script_name, engine=self.engine, is_exists=is_exists)
                    
                result["success"] = True

        except Exception as e:
            self.logger.error(f"执行脚本 {script_name} 的函数 {func_name} 失败: {str(e)}")
            result["errors"].append(str(e))
            
        finally:
            # 保存最终结果到JSON文件
            result["error_count"] = len(result["errors"])
            result["finish_time"] = datetime.now()
            self._save_result_to_json(actual_script_name, result)
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

            is_test = self._is_test_script(script_name)

            script_info = {
                "name": script_name,
                "file_path": str(script_file),
                "modified_time": datetime.fromtimestamp(script_file.stat().st_mtime),
                "is_test": is_test,
            }

            if is_test:
                scripts_info["test_scripts"].append(script_info)
            else:
                scripts_info["regular_scripts"].append(script_info)

        scripts_info["total"] = len(scripts_info["regular_scripts"]) + len(
            scripts_info["test_scripts"]
        )

        return scripts_info

    def _get_or_create_script_schedule(self, script_name: str) -> ScriptSchedule:
        """
        获取或创建 ScriptSchedule 对象

        Args:
            script_name (str): 脚本名称

        Returns:
            ScriptSchedule: 脚本调度对象
        """
        try:
            # 获取数据库引擎
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                # 尝试查找现有的 ScriptSchedule
                script_schedule = (
                    session.query(ScriptSchedule)
                    .filter(ScriptSchedule.name == script_name)
                    .first()
                )

                if script_schedule is None:
                    # 创建新的 ScriptSchedule
                    script_schedule = ScriptSchedule(
                        name=script_name,
                        period="",
                        turn_on=False,
                        remark=f"自动创建的脚本调度记录 - {script_name}",
                    )
                    session.add(script_schedule)
                    session.commit()
                    session.refresh(script_schedule)
                    self.logger.info(f"创建新的 ScriptSchedule: {script_name}")

                return script_schedule

        except Exception as e:
            self.logger.error(f"获取或创建 ScriptSchedule 失败: {str(e)}")
            # 返回一个基本的 ScriptSchedule 对象
            return ScriptSchedule(name=script_name)

    def _store_execution_result(self, script_name: str, result: Any) -> bool:
        """
        存储执行结果到数据库

        Args:
            script_name (str): 脚本名称
            result (Any): 执行结果
            script_schedule (ScriptSchedule): 脚本调度对象

        Returns:
            bool: 是否存储成功
        """
        try:
            engines = config.init_db()
            engine = engines["script_engine"]

            # 如果结果是 DataFrame，使用现有的 DataFrame 存储方法
            if isinstance(result, pd.DataFrame):
                return self._store_dataframe_to_db(result, script_name, engine)

        except Exception as e:
            self.logger.error(f"存储执行结果失败: {str(e)}")
            return False

    def _log_execution_result(self, script_name: str, result: Any) -> None:
        """
        将执行结果记录到日志文件

        Args:
            script_name (str): 脚本名称
            result (Any): 执行结果
        """
        try:

            # 创建日志目录
            log_dir = config.get_script_log_dir(script_name)
            os.makedirs(log_dir, exist_ok=True)

            # 生成日志文件名（按日期）
            log_file = os.path.join(
                log_dir, f"execution_{datetime.now().strftime('%Y%m%d')}.log"
            )

            # 准备日志内容
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "script_name": script_name,
                "execution_time": datetime.now().isoformat(),
                "result_type": type(result).__name__,
                "result_summary": (
                    str(result)[:500] if result is not None else None
                ),  # 限制长度
                "success": True,
            }

            # 尝试将结果转换为 JSON
            try:
                if hasattr(result, "to_dict"):
                    # 处理 DataFrame，转换时间戳
                    result_dict = result.to_dict()
                    # 递归处理可能的时间戳对象
                    log_entry["result_data"] = self._serialize_for_json(result_dict)
                elif isinstance(result, (dict, list)):
                    log_entry["result_data"] = self._serialize_for_json(result)
                else:
                    log_entry["result_data"] = str(result)
            except Exception as e:
                log_entry["result_data"] = f"无法序列化的结果: {str(e)}"

            # 写入日志文件
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

        except Exception as e:
            self.logger.error(f"记录执行日志失败: {str(e)}")

    def _serialize_for_json(self, obj):
        """
        将对象转换为JSON可序列化的格式

        Args:
            obj: 要序列化的对象

        Returns:
            JSON可序列化的对象
        """
        import pandas as pd
        from datetime import datetime, date

        if isinstance(obj, dict):
            return {key: self._serialize_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        else:
            return obj

    def _save_result_to_json(self, script_name: str, result: Any) -> bool:
        """
        将脚本执行结果保存为JSON文件

        Args:
            script_name (str): 脚本名称，用作文件名
            result (Any): 要保存的执行结果

        Returns:
            bool: 是否保存成功
        """
        try:
            # 创建数据目录（如果不存在）
            data_dir = Path(config.base_dir) / "data" / script_name
            data_dir.mkdir(parents=True, exist_ok=True)

            # 生成JSON文件路径
            json_file_path = data_dir / f"{script_name}.json"

            # 保存为JSON文件
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)

            self.logger.info(f"成功将脚本 {script_name} 的执行结果保存到: {json_file_path}")
            return True

        except Exception as e:
            self.logger.error(f"保存脚本 {script_name} 执行结果到JSON文件失败: {str(e)}")
            return False

    def _load_result_from_json(self, script_name: str) -> Optional[Dict[str, Any]]:
        """
        从JSON文件中读取脚本执行结果

        Args:
            script_name (str): 脚本名称，用作文件名

        Returns:
            Optional[Dict[str, Any]]: 读取到的执行结果和元数据字典，如果读取失败返回None
                包含键: result(执行结果), script_name(脚本名称), execution_time(执行时间), result_type(结果类型)
        """
        try:
            # 生成JSON文件路径
            data_dir = Path(config.base_dir) / "data" / script_name
            json_file_path = data_dir / f"{script_name}_result.json"

            # 检查文件是否存在
            if not json_file_path.exists():
                self.logger.warning(f"脚本 {script_name} 的JSON文件不存在: {json_file_path}")
                return None

            # 从JSON文件读取数据
            with open(json_file_path, 'r', encoding='utf-8') as f:
                saved_data = json.load(f)
            
            self.logger.info(f"成功从JSON文件读取脚本 {script_name} 的执行结果")
            return saved_data

        except json.JSONDecodeError as e:
            self.logger.error(f"解析脚本 {script_name} 的JSON文件失败: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"读取脚本 {script_name} 的执行结果失败: {str(e)}")
            return None

    def get_script_result(self, script_name: str) -> Optional[Any]:
        """
        获取脚本的最新执行结果（便捷方法）

        Args:
            script_name (str): 脚本名称

        Returns:
            Optional[Any]: 脚本执行结果，如果不存在或读取失败返回None
        """
        result = self._load_result_from_json(script_name)
        return result

    def has_saved_result(self, script_name: str) -> bool:
        """
        检查脚本是否有保存的执行结果

        Args:
            script_name (str): 脚本名称

        Returns:
            bool: 是否有保存的结果
        """
        try:
            data_dir = Path(config.base_dir) / "data" / script_name
            json_file_path = data_dir / f"{script_name}.json"
            return json_file_path.exists()
        except Exception:
            return False

    def convert_menu(self, menu_path: str = None) -> Dict[str, Any]:
        """
        将 Menu.json 转换为脚本调度配置并更新数据库

        Args:
            menu_path (str): Menu.json 文件路径，如果为 None 则使用项目根目录的 Menu.json

        Returns:
            Dict[str, Any]: 转换和更新结果
        """
        result = {
            "success": False,
            "menu_path": "",
            "total_items": 0,
            "created_items": 0,
            "updated_items": 0,
            "skipped_items": 0,
            "message": "",
            "details": [],
        }

        try:
            # 1. 确定 Menu.json 路径
            if menu_path is None:
                menu_path = os.path.join(config.base_dir, "Menu.json")
            result["menu_path"] = menu_path

            self.logger.info(f"开始转换 Menu.json: {menu_path}")

            # 2. 导入工具模块并调用转换函数
            tools_dir = os.path.join(config.base_dir, "tools", "sys")
            if not os.path.exists(tools_dir):
                raise FileNotFoundError(f"工具目录不存在: {tools_dir}")

            # 添加工具目录到 Python 路径
            if tools_dir not in sys.path:
                sys.path.insert(0, tools_dir)

            try:
                from menu2script_schedule import convert_menu_to_script_schedule

                self.logger.info("成功导入转换工具函数")
            except ImportError as e:
                raise ImportError(f"无法导入 menu2script_schedule 工具: {str(e)}")

            # 3. 执行转换
            schedule_df = convert_menu_to_script_schedule(menu_path)

            if schedule_df is False or schedule_df.empty:
                raise ValueError("转换失败或返回空数据")

            result["total_items"] = len(schedule_df)
            self.logger.info(f"转换完成，共生成 {result['total_items']} 个调度条目")

            # 4. 获取数据库连接
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                for _, row in schedule_df.iterrows():
                    script_name = row["name"]
                    self.logger.info(f"处理脚本: {script_name}")

                    # 检查是否已存在
                    existing_schedule = (
                        session.query(ScriptSchedule)
                        .filter(ScriptSchedule.name == script_name)
                        .first()
                    )

                    if existing_schedule:
                        # 保留原有的 last_sync_datetime
                        original_last_sync = existing_schedule.last_sync_datetime

                        # 更新字段
                        existing_schedule.period = row.get("period", "")
                        existing_schedule.turn_on = row.get("turn_on", False)
                        existing_schedule.next_sync_datetime = None  # 转换时重置
                        existing_schedule.updated_at = datetime.now()

                        # 只在原值为空时才设置新的 last_sync_datetime
                        if original_last_sync is None and row.get("last_sync_datetime"):
                            existing_schedule.last_sync_datetime = row.get(
                                "last_sync_datetime"
                            )

                        session.add(existing_schedule)
                        result["updated_items"] += 1

                        detail = {
                            "script_name": script_name,
                            "action": "updated",
                            "last_sync_preserved": original_last_sync is not None,
                            "period": existing_schedule.period,
                            "turn_on": existing_schedule.turn_on,
                        }
                        result["details"].append(detail)

                        self.logger.info(
                            f"更新脚本调度: {script_name} (保留 last_sync_datetime: {original_last_sync is not None})"
                        )

                    else:
                        # 创建新记录
                        new_schedule = ScriptSchedule(
                            name=script_name,
                            period=row.get("period", ""),
                            turn_on=row.get("turn_on", False),
                            last_sync_datetime=None,  # 新记录没有最后同步时间
                            next_sync_datetime=None,
                            remark=f"从 Menu.json 自动创建 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        )
                        session.add(new_schedule)
                        result["created_items"] += 1

                        detail = {
                            "script_name": script_name,
                            "action": "created",
                            "period": new_schedule.period,
                            "turn_on": new_schedule.turn_on,
                        }
                        result["details"].append(detail)

                        self.logger.info(f"创建新脚本调度: {script_name}")

                # 提交事务
                session.commit()

            # 5. 生成结果消息
            message_parts = []
            if result["created_items"] > 0:
                message_parts.append(f"创建了 {result['created_items']} 个新条目")
            if result["updated_items"] > 0:
                message_parts.append(f"更新了 {result['updated_items']} 个条目")

            result["message"] = (
                f"Menu.json 转换成功！共处理 {result['total_items']} 个条目，{', '.join(message_parts)}"
            )
            result["success"] = True

            self.logger.info(f"Menu.json 转换完成: {result['message']}")

        except Exception as e:
            error_msg = f"Menu.json 转换失败: {str(e)}"
            result["message"] = error_msg
            self.logger.error(error_msg, exc_info=True)

        return result
