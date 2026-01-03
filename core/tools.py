# 用于存放各类工具函数
import sys
import importlib.util
from typing import Any, Optional, Dict
from datetime import datetime
from pathlib import Path
import pandas as pd
from croniter import croniter
import logging
import json
from sqlmodel import Session

from .config import config
from .models import ScriptSchedule


def calculate_next_sync_time(
    last_sync_time: datetime, 
    cron_expression: str, 
    logger: logging.Logger
) -> Optional[datetime]:
    """
    根据上次执行时间和crontab表达式计算下次执行时间

    Args:
        last_sync_time (datetime): 上次执行时间
        cron_expression (str): crontab表达式
        logger (logging.Logger): 日志记录器

    Returns:
        Optional[datetime]: 下次执行时间，计算失败返回None
    """
    if not cron_expression:
        logger.error("crontab表达式为空")
        return None
    try:
        # 使用croniter解析表达式并计算下次执行时间
        cron = croniter(cron_expression, last_sync_time)
        next_time = cron.get_next(datetime)
        return next_time
    except Exception as e:
        logger.error(f"计算下次执行时间失败: {str(e)}")
        return None


def import_script(
    script_name: str, 
    scripts_dir: Path, 
    logger: logging.Logger
) -> Any:
    """
    动态导入脚本模块

    Args:
        script_name (str): 脚本名称（不含.py扩展名）
        scripts_dir (Path): 脚本目录路径
        logger (logging.Logger): 日志记录器

    Returns:
        Any: 导入的模块

    Raises:
        ImportError: 脚本文件不存在或导入失败
    """
    script_file = scripts_dir / f"{script_name}.py"

    if not script_file.exists():
        raise FileNotFoundError(f"脚本文件不存在: {script_file}")

    # 动态导入脚本
    spec = importlib.util.spec_from_file_location(script_name, script_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载脚本规范: {script_name}")

    module = importlib.util.module_from_spec(spec)

    # 添加脚本目录到Python路径
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    try:
        spec.loader.exec_module(module)
        logger.info(f"成功导入脚本: {script_name}")
        return module
    except Exception as e:
        logger.error(f"导入脚本失败 {script_name}: {str(e)}")
        raise ImportError(f"导入脚本失败 {script_name}: {str(e)}")


def store_dataframe_to_db(
    df: pd.DataFrame, 
    table_name: str, 
    engine, 
    logger: logging.Logger,
    is_exists: str = "append"
) -> bool:
    """
    将DataFrame存储到数据库

    Args:
        df (pd.DataFrame): 要存储的数据
        table_name (str): 表名
        engine: 数据库引擎
        logger (logging.Logger): 日志记录器
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
        logger.info(f"成功存储 {records} 条记录到表 {table_name}")
        return True

    except Exception as e:
        logger.error(f"存储DataFrame到数据库失败: {str(e)}")
        return False


def save_result_to_json(script_name: str, result: Any, logger: logging.Logger) -> bool:
    """
    将脚本执行结果保存为JSON文件

    Args:
        script_name (str): 脚本名称，用作文件名
        result (Any): 要保存的执行结果
        logger (logging.Logger): 日志记录器

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

        logger.info(f"成功将脚本 {script_name} 的执行结果保存到: {json_file_path}")
        return True

    except Exception as e:
        logger.error(f"保存脚本 {script_name} 执行结果到JSON文件失败: {str(e)}")
        return False


def load_result_from_json(script_name: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    从JSON文件中读取脚本执行结果

    Args:
        script_name (str): 脚本名称，用作文件名
        logger (logging.Logger): 日志记录器

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
            logger.warning(f"脚本 {script_name} 的JSON文件不存在: {json_file_path}")
            return None

        # 从JSON文件读取数据
        with open(json_file_path, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        
        logger.info(f"成功从JSON文件读取脚本 {script_name} 的执行结果")
        return saved_data

    except json.JSONDecodeError as e:
        logger.error(f"解析脚本 {script_name} 的JSON文件失败: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"读取脚本 {script_name} 的执行结果失败: {str(e)}")
        return None


def get_script_result(script_name: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    获取脚本的最新执行结果（便捷方法）

    Args:
        script_name (str): 脚本名称
        logger (logging.Logger): 日志记录器

    Returns:
        Optional[Dict[str, Any]]: 脚本执行结果，如果不存在或读取失败返回None
    """
    return load_result_from_json(script_name, logger)


def has_saved_result(script_name: str) -> bool:
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


def get_or_create_script_schedule(script_name: str, logger: logging.Logger) -> ScriptSchedule:
    """
    获取或创建 ScriptSchedule 对象

    Args:
        script_name (str): 脚本名称
        logger (logging.Logger): 日志记录器

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
                logger.info(f"创建新的 ScriptSchedule: {script_name}")

            return script_schedule

    except Exception as e:
        logger.error(f"获取或创建 ScriptSchedule 失败: {str(e)}")
        # 返回一个基本的 ScriptSchedule 对象
        return ScriptSchedule(name=script_name)


def store_execution_result(script_name: str, result: Any, logger: logging.Logger) -> bool:
    """
    存储执行结果到数据库

    Args:
        script_name (str): 脚本名称
        result (Any): 执行结果
        logger (logging.Logger): 日志记录器

    Returns:
        bool: 是否存储成功
    """
    try:
        engines = config.init_db()
        engine = engines["script_engine"]

        # 如果结果是 DataFrame，使用现有的 DataFrame 存储方法
        if isinstance(result, pd.DataFrame):
            return store_dataframe_to_db(result, script_name, engine, logger)

    except Exception as e:
        logger.error(f"存储执行结果失败: {str(e)}")
        return False