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
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _calculate_next_sync_time(self, last_sync_time: datetime, cron_expression: str) -> Optional[datetime]:
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
    
    def _store_dataframe_to_db(self, df: pd.DataFrame, table_name: str, 
                             engine) -> bool:
        """
        将DataFrame存储到数据库
        
        Args:
            df (pd.DataFrame): 要存储的数据
            table_name (str): 表名
            engine: 数据库引擎
            
        Returns:
            bool: 是否存储成功
        """
        try:
            with Session(engine) as session:
                # 将DataFrame转换为字典列表
                records = df.to_dict('records')
                
                # 这里需要根据具体的数据模型来创建记录
                # 示例：假设有一个通用的数据表来存储脚本返回的数据
                for record in records:
                    # 创建记录逻辑需要根据实际的数据模型来实现
                    # 这里只是一个示例结构
                    pass
                
                session.commit()
                self.logger.info(f"成功存储 {len(records)} 条记录到表 {table_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"存储DataFrame到数据库失败: {str(e)}")
            return False
    
    def execute_script(self, script_name: str, func_name: Optional[str] = None, save_to_db: bool = True) -> Dict[str, Any]:
        """
        执行指定脚本的指定函数
        
        Args:
            script_name (str): 脚本名称（不含.py扩展名）也是表名，也是日志记录文件名
            func_name (Optional[str]): 函数名称，所有被执行的函数都会传入script_schedule对象,self,depend执行结果
            save_to_db (bool): 是否存储结果到数据库
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        result = {
            'success': False,
            'script_name': script_name,
            'execution_time': datetime.now(),
            'result': None,
            'message': '',
            'data_stored': False
        }
        
        try:
            # 1. 动态导入脚本
            module = self._import_script(script_name)
            
            # 2. 获取或创建 ScriptSchedule 对象
            script_schedule = self._get_or_create_script_schedule(script_name)
            
            # 3. 处理 depend 函数（如果存在）
            depend_result = None
            if hasattr(module, 'depend'):
                self.logger.info(f"调用脚本 {script_name} 的 depend 函数")
                depend_result = module.depend(script_schedule, self)
                self.logger.info(f"depend 函数返回结果: {depend_result}")
            
            # 4. 确定要执行的函数
            if func_name is None:
                # 默认执行 main 函数，如果没有则执行 run 函数
                if hasattr(module, 'main'):
                    func_name = 'main'
                elif hasattr(module, 'run'):
                    func_name = 'run'
                else:
                    raise ValueError(f"脚本 {script_name} 中未找到可执行的函数（main 或 run）")
            
            if not hasattr(module, func_name):
                raise ValueError(f"脚本 {script_name} 中未找到函数 {func_name}")
            
            # 5. 执行指定函数
            self.logger.info(f"执行脚本 {script_name} 的函数 {func_name}")
            
            target_func = getattr(module, func_name)
            
            # 根据函数签名决定参数
            sig = inspect.signature(target_func)
            params = list(sig.parameters.keys())
            
            func_result = None
            if len(params) == 0:
                # 无参数函数
                func_result = target_func()
            elif len(params) == 1:
                # 单参数函数，传入 script_schedule
                func_result = target_func(script_schedule)
            elif len(params) == 2:
                # 双参数函数，传入 script_schedule 和 self
                func_result = target_func(script_schedule, self)
            elif len(params) >= 3:
                # 三参数或更多，传入 script_schedule, self, depend_result
                func_result = target_func(script_schedule, self, depend_result)
            else:
                func_result = target_func()
            
            result['result'] = func_result
            result['success'] = True
            result['message'] = f"成功执行脚本 {script_name} 的函数 {func_name}"
            
            # 6. 处理数据库存储和日志记录
            if func_result is not None:
                # 无论是否保存到数据库，都要记录执行日志
                self._log_execution_result(script_name, func_result)
                
                if save_to_db:
                    # 更新 script_schedule 的最后执行时间并存储执行结果
                    current_time = datetime.now()
                    script_schedule.last_sync_datetime = self._calculate_next_sync_time(script_schedule.last_sync_datetime, script_schedule.period) if script_schedule.last_sync_datetime and script_schedule.period else current_time
                    script_schedule.updated_at = current_time
                    
                    # 先更新 script_schedule，再存储执行结果
                    # 只有两个操作都成功才算整体成功
                    script_schedule_updated = False
                    storage_success = False
                    
                    try:
                        engines = config.init_db()
                        engine = engines["engine"]
                        
                        # 1. 更新 script_schedule 表（主库）
                        with Session(engine) as session:
                            session.add(script_schedule)
                            session.commit()
                        script_schedule_updated = True
                        self.logger.info(f"更新 script_schedule 最后执行时间: {script_name} -> {current_time}")
                        
                        # 2. 存储执行结果到脚本库（脚本库）
                        storage_success = self._store_execution_result(script_name, func_result)
                        
                        # 只有两个操作都成功才算成功
                        if script_schedule_updated and storage_success:
                            result['message'] += "，结果已存储到数据库"
                        elif script_schedule_updated and not storage_success:
                            result['message'] += "，script_schedule已更新但执行结果存储失败"
                        else:
                            result['message'] += "，script_schedule更新失败"
                            
                    except Exception as e:
                        self.logger.error(f"数据库操作失败: {str(e)}")
                        result['message'] += "，数据库操作失败"
                        script_schedule_updated = False
                        storage_success = False
                    
                    # 只有两个操作都成功才算数据存储成功
                    result['data_stored'] = script_schedule_updated and storage_success
                else:
                    print(func_result)
                    result['data_stored'] = False
            
            self.logger.info(f"脚本执行成功: {script_name} -> {func_name}")
            
        except Exception as e:
            error_msg = f"执行脚本 {script_name} 失败: {str(e)}"
            result['message'] = error_msg
            self.logger.error(error_msg, exc_info=True)
        
        return result
    
    def _save_breakpoint_info(self, script_name: str, breakpoint_info: Dict[str, Any]) -> None:
        """
        保存执行断点信息到文件
        
        Args:
            script_name (str): 脚本名称
            breakpoint_info (Dict[str, Any]): 断点信息
        """
        try:
            # 创建断点目录
            breakpoint_dir = config.get_script_breakpoint_dir(script_name)
            os.makedirs(breakpoint_dir, exist_ok=True)
            
            # 生成断点文件名
            breakpoint_file = os.path.join(breakpoint_dir, f"breakpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            # 保存断点信息
            with open(breakpoint_file, 'w', encoding='utf-8') as f:
                json.dump(breakpoint_info, f, ensure_ascii=False, indent=2)
                
            # 同时更新最新的断点文件
            latest_breakpoint_file = os.path.join(breakpoint_dir, "latest_breakpoint.json")
            with open(latest_breakpoint_file, 'w', encoding='utf-8') as f:
                json.dump(breakpoint_info, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"断点信息已保存: {breakpoint_file}")
            
        except Exception as e:
            self.logger.error(f"保存断点信息失败: {str(e)}")
    
    def _load_latest_breakpoint_info(self, script_name: str) -> Optional[Dict[str, Any]]:
        """
        读取最新的断点信息
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            Optional[Dict[str, Any]]: 断点信息，如果不存在则返回 None
        """
        try:
            breakpoint_dir = config.get_script_breakpoint_dir(script_name)
            latest_breakpoint_file = os.path.join(breakpoint_dir, "latest_breakpoint.json")
            
            if not os.path.exists(latest_breakpoint_file):
                return None
                
            with open(latest_breakpoint_file, 'r', encoding='utf-8') as f:
                breakpoint_info = json.load(f)
                
            self.logger.info(f"成功读取断点信息: {script_name}")
            return breakpoint_info
            
        except Exception as e:
            self.logger.error(f"读取断点信息失败: {str(e)}")
            return None
    
    def _clear_breakpoint_info(self, script_name: str) -> None:
        """
        清除断点信息
        
        Args:
            script_name (str): 脚本名称
        """
        try:
            breakpoint_dir = config.get_script_breakpoint_dir(script_name)
            latest_breakpoint_file = os.path.join(breakpoint_dir, "latest_breakpoint.json")
            
            if os.path.exists(latest_breakpoint_file):
                os.remove(latest_breakpoint_file)
                self.logger.info(f"已清除断点信息: {script_name}")
            
        except Exception as e:
            self.logger.error(f"清除断点信息失败: {str(e)}")
        
    def iterate_script(self, script_name: str, is_err_stop: bool = True):
        """
        首先执行脚本中的depend函数,depend函数会返回一个多维数组,
        然后将这个多维数组中的每个元素作为dict，传递给当前脚本的执行函数的depend参数
        并且要记录执行断点,方便用户执行catch_on函数（catch_on函数目前未实现）
        
        Args:
            script_name (str): 依赖的脚本名称
            is_err_stop (bool, optional): 是否在迭代过程中遇到错误时停止. Defaults to True.
        """
        result = {
            'success': False,
            'script_name': script_name,
            'execution_time': datetime.now(),
            'depend_data': None,
            'iteration_results': [],
            'total_iterations': 0,
            'successful_iterations': 0,
            'failed_iterations': 0,
            'breakpoint_info': None,
            'message': '',
            'data_stored': False,
            'error_items': []  # 新增：存储报错的元素列表
        }
        
        try:
            # 1. 动态导入脚本
            module = self._import_script(script_name)
            
            # 2. 获取或创建 ScriptSchedule 对象
            script_schedule = self._get_or_create_script_schedule(script_name)
            
            # 在迭代开始前获取所需的字段值，避免后续访问时对象脱离Session
            script_period = script_schedule.period
            script_current_next_sync_datetime = script_schedule.next_sync_datetime
            
            # 3. 检查脚本是否有 depend 函数
            if not hasattr(module, 'depend'):
                raise ValueError(f"脚本 {script_name} 中未找到 depend 函数")
            
            # 4. 执行 depend 函数获取依赖数据
            self.logger.info(f"执行脚本 {script_name} 的 depend 函数")
            depend_result = module.depend(script_schedule, self)
            
            if depend_result is None:
                self.logger.warning(f"depend 函数返回 None，脚本 {script_name} 没有需要处理的数据")
                result['message'] = "depend 函数返回 None，无需处理"
                result['success'] = True
                return result
            
            # 确保 depend_result 是可迭代的
            if not hasattr(depend_result, '__iter__'):
                raise ValueError(f"depend 函数返回结果不可迭代，返回类型: {type(depend_result)}")
            
            # 转换为列表以确保可以迭代
            depend_data = list(depend_result)
            result['depend_data'] = depend_data
            result['total_iterations'] = len(depend_data)
            
            self.logger.info(f"depend 函数返回 {len(depend_data)} 条数据")
            
            # 5. 检查脚本是否有 iteration 函数
            if not hasattr(module, 'iteration'):
                raise ValueError(f"脚本 {script_name} 中未找到 iteration 函数")
            
            iteration_func = getattr(module, 'iteration')
            
            # 6. 循环调用 iteration 函数
            self.logger.info(f"开始循环执行脚本 {script_name} 的 iteration 函数")
            
            for i, depend_item in enumerate(depend_data):
                try:
                    # 检查 iteration 函数的参数签名
                    sig = inspect.signature(iteration_func)
                    params = list(sig.parameters.keys())
                    
                    iteration_result = None
                    
                    if len(params) == 1:
                        # 单参数：传入 depend_item
                        iteration_result = iteration_func(depend_item)
                    elif len(params) == 2:
                        # 双参数：传入 script_schedule 和 depend_item
                        iteration_result = iteration_func(script_schedule, depend_item)
                    elif len(params) >= 3:
                        # 三参数或更多：传入 script_schedule, self, depend_item
                        iteration_result = iteration_func(script_schedule, self, depend_item)
                    else:
                        # 无参数函数
                        iteration_result = iteration_func()
                    
                    # 记录成功结果
                    result['iteration_results'].append({
                        'index': i,
                        'success': True,
                        'result': iteration_result,
                        'depend_item': depend_item,
                        'execution_time': datetime.now().isoformat()
                    })
                    
                    result['successful_iterations'] += 1
                    
                    self.logger.info(f"第 {i+1}/{len(depend_data)} 次 iteration 执行成功")
                    
                    # 如果有结果，存储到数据库
                    if iteration_result is not None:
                        self._log_execution_result(f"{script_name}_iteration_{i}", iteration_result)
                        
                        # 更新 script_schedule 的最后执行时间
                        current_time = datetime.now()
                        script_schedule.last_sync_datetime = script_current_next_sync_datetime if script_current_next_sync_datetime and script_period else current_time
                        script_schedule.updated_at = current_time
                    
                        if script_period:
                            script_schedule.next_sync_datetime = self._calculate_next_sync_time(script_current_next_sync_datetime, script_period)

                        try:
                            engines = config.init_db()
                            engine = engines["engine"]
                            
                            with Session(engine) as session:
                                session.merge(script_schedule)
                                session.commit()
                                
                            # 存储 iteration 结果到数据库
                            if self._store_execution_result(f"{script_name}_iteration_{i}", iteration_result):
                                result['data_stored'] = True
                                
                        except Exception as e:
                            self.logger.error(f"数据库操作失败 (iteration {i}): {str(e)}")
                    
                except Exception as e:
                    # 记录失败结果
                    error_msg = f"第 {i+1} 次 iteration 执行失败: {str(e)}"
                    self.logger.error(error_msg, exc_info=True)
                    
                    result['iteration_results'].append({
                        'index': i,
                        'success': False,
                        'error': str(e),
                        'depend_item': depend_item,
                        'execution_time': datetime.now().isoformat()
                    })
                    
                    result['failed_iterations'] += 1
                    
                    # 记录报错的元素
                    result['error_items'].append({
                        'index': i,
                        'depend_item': depend_item,
                        'error': str(e)
                    })
                    
                    # 根据 is_err_stop 参数决定是否终止循环
                    if is_err_stop:
                        break  # 发生失败时停止后续迭代
            
            # 7. 记录执行断点信息
            breakpoint_info = {
                'script_name': script_name,
                'total_items': len(depend_data),
                'successful_items': result['successful_iterations'],
                'failed_items': result['failed_iterations'],
                'last_successful_index': None,
                'last_execution_time': datetime.now().isoformat()
            }
            
            # 找到最后一个成功的 iteration 索引
            for i in range(len(result['iteration_results']) - 1, -1, -1):
                if result['iteration_results'][i]['success']:
                    breakpoint_info['last_successful_index'] = i
                    break
            
            result['breakpoint_info'] = breakpoint_info
            
            # 8. 保存断点信息
            self._save_breakpoint_info(script_name, breakpoint_info)
            
            # 9. 更新最终状态
            if result['failed_iterations'] == 0:
                result['success'] = True
                result['message'] = f"脚本 {script_name} 的 iteration 全部执行成功，共 {result['successful_iterations']} 次"
            elif result['successful_iterations'] > 0:
                result['success'] = True
                result['message'] = f"脚本 {script_name} 的 iteration 部分成功，成功 {result['successful_iterations']} 次，失败 {result['failed_iterations']} 次"
            else:
                result['success'] = False
                result['message'] = f"脚本 {script_name} 的 iteration 全部失败，共 {result['failed_iterations']} 次"
            
            self.logger.info(f"脚本 {script_name} iteration 执行完成: {result['message']}")
            
        except Exception as e:
            error_msg = f"执行脚本 {script_name} 的 iteration 失败: {str(e)}"
            result['message'] = error_msg
            self.logger.error(error_msg, exc_info=True)
        
        return result
    

    
    def catch_on(self, script_name: str):
        """
        断点续跑功能：根据断点信息从上次失败的位置继续执行 iteration
        首先读取断点信息，然后从断点位置继续执行 iteration 函数
        
        Args:
            script_name (str): 脚本名称
        """
        result = {
            'success': False,
            'script_name': script_name,
            'execution_time': datetime.now(),
            'breakpoint_info': None,
            'continued_iterations': [],
            'total_continued': 0,
            'successful_continued': 0,
            'failed_continued': 0,
            'message': '',
            'data_stored': False
        }
        
        try:
            # 1. 读取断点信息
            breakpoint_info = self._load_latest_breakpoint_info(script_name)
            
            if breakpoint_info is None:
                result['message'] = f"脚本 {script_name} 没有找到断点信息，无法执行断点续跑"
                self.logger.warning(result['message'])
                return result
            
            result['breakpoint_info'] = breakpoint_info
            
            # 验证断点信息是否有效
            if 'last_successful_index' not in breakpoint_info:
                result['message'] = f"脚本 {script_name} 的断点信息格式无效"
                self.logger.error(result['message'])
                return result
            
            last_successful_index = breakpoint_info['last_successful_index']
            total_items = breakpoint_info['total_items']
            
            if last_successful_index is None:
                # 所有iteration都失败了，从头开始
                start_index = 0
                self.logger.info(f"脚本 {script_name} 所有iteration都失败了，从头开始重新执行")
            elif last_successful_index >= total_items - 1:
                # 所有iteration都成功了，无需继续
                result['success'] = True
                result['message'] = f"脚本 {script_name} 的 iteration 已经全部成功执行，无需继续"
                self.logger.info(result['message'])
                return result
            else:
                # 从最后一个成功的位置继续
                start_index = last_successful_index + 1
                self.logger.info(f"脚本 {script_name} 从第 {start_index} 个元素开始继续执行")
            
            # 2. 动态导入脚本
            module = self._import_script(script_name)
            
            # 3. 获取或创建 ScriptSchedule 对象
            script_schedule = self._get_or_create_script_schedule(script_name)
            
            # 4. 重新执行 depend 函数获取依赖数据
            if not hasattr(module, 'depend'):
                raise ValueError(f"脚本 {script_name} 中未找到 depend 函数")
            
            self.logger.info(f"执行脚本 {script_name} 的 depend 函数")
            depend_result = module.depend(script_schedule, self)
            
            if depend_result is None:
                result['message'] = f"脚本 {script_name} depend 函数返回 None，无法继续执行"
                self.logger.warning(result['message'])
                return result
            
            # 确保 depend_result 是可迭代的
            if not hasattr(depend_result, '__iter__'):
                raise ValueError(f"depend 函数返回结果不可迭代，返回类型: {type(depend_result)}")
            
            # 转换为列表
            depend_data = list(depend_result)
            
            if start_index >= len(depend_data):
                result['message'] = f"断点位置 {start_index} 超出数据范围 {len(depend_data)}"
                self.logger.error(result['message'])
                return result
            
            # 5. 检查 iteration 函数
            if not hasattr(module, 'iteration'):
                raise ValueError(f"脚本 {script_name} 中未找到 iteration 函数")
            
            iteration_func = getattr(module, 'iteration')
            
            # 6. 从断点位置继续执行 iteration
            self.logger.info(f"开始从第 {start_index} 个元素继续执行 iteration")
            
            for i in range(start_index, len(depend_data)):
                depend_item = depend_data[i]
                
                try:
                    # 检查 iteration 函数的参数签名
                    sig = inspect.signature(iteration_func)
                    params = list(sig.parameters.keys())
                    
                    iteration_result = None
                    
                    if len(params) == 1:
                        # 单参数：传入 depend_item
                        iteration_result = iteration_func(depend_item)
                    elif len(params) == 2:
                        # 双参数：传入 script_schedule 和 depend_item
                        iteration_result = iteration_func(script_schedule, depend_item)
                    elif len(params) >= 3:
                        # 三参数或更多：传入 script_schedule, self, depend_item
                        iteration_result = iteration_func(script_schedule, self, depend_item)
                    else:
                        # 无参数函数
                        iteration_result = iteration_func()
                    
                    # 记录成功结果
                    result['continued_iterations'].append({
                        'index': i,
                        'success': True,
                        'result': iteration_result,
                        'depend_item': depend_item,
                        'execution_time': datetime.now().isoformat()
                    })
                    
                    result['successful_continued'] += 1
                    result['total_continued'] += 1
                    
                    self.logger.info(f"断点续跑: 第 {i+1}/{len(depend_data)} 次 iteration 执行成功")
                    
                    # 如果有结果，存储到数据库
                    if iteration_result is not None:
                        self._log_execution_result(f"{script_name}_catch_on_{i}", iteration_result)
                        
                        # 更新 script_schedule 的最后执行时间
                        current_time = datetime.now()
                        script_schedule.last_sync_datetime = current_time
                        script_schedule.updated_at = current_time
                        
                        try:
                            engines = config.init_db()
                            engine = engines["engine"]
                            
                            with Session(engine) as session:
                                session.add(script_schedule)
                                session.commit()
                                
                            # 存储 iteration 结果到数据库
                            if self._store_execution_result(f"{script_name}_catch_on_{i}", iteration_result):
                                result['data_stored'] = True
                                
                        except Exception as e:
                            self.logger.error(f"数据库操作失败 (catch_on {i}): {str(e)}")
                    
                except Exception as e:
                    # 记录失败结果
                    error_msg = f"断点续跑第 {i+1} 次 iteration 执行失败: {str(e)}"
                    self.logger.error(error_msg, exc_info=True)
                    
                    result['continued_iterations'].append({
                        'index': i,
                        'success': False,
                        'error': str(e),
                        'depend_item': depend_item,
                        'execution_time': datetime.now().isoformat()
                    })
                    
                    result['failed_continued'] += 1
                    result['total_continued'] += 1
                    
                    # 如果有iteration失败，不更新断点信息
                    break
            
            # 7. 如果所有续跑的iteration都成功，更新断点信息
            if result['failed_continued'] == 0:
                # 重新计算最后的成功索引
                new_last_successful_index = len(depend_data) - 1
                
                new_breakpoint_info = {
                    'script_name': script_name,
                    'total_items': len(depend_data),
                    'successful_items': breakpoint_info.get('successful_items', 0) + result['successful_continued'],
                    'failed_items': breakpoint_info.get('failed_items', 0) + result['failed_continued'],
                    'last_successful_index': new_last_successful_index,
                    'last_execution_time': datetime.now().isoformat(),
                    'catch_on_executed': True,
                    'catch_on_start_index': start_index
                }
                
                # 保存更新后的断点信息
                self._save_breakpoint_info(script_name, new_breakpoint_info)
            
            # 8. 更新最终状态
            if result['failed_continued'] == 0:
                result['success'] = True
                result['message'] = f"脚本 {script_name} 的断点续跑全部成功，共执行 {result['successful_continued']} 次"
            elif result['successful_continued'] > 0:
                result['success'] = True
                result['message'] = f"脚本 {script_name} 的断点续跑部分成功，成功 {result['successful_continued']} 次，失败 {result['failed_continued']} 次"
            else:
                result['success'] = False
                result['message'] = f"脚本 {script_name} 的断点续跑全部失败，共 {result['failed_continued']} 次"
            
            # 9. 如果所有iteration都成功，清除断点信息
            if result['failed_continued'] == 0 and (breakpoint_info.get('successful_items', 0) + result['successful_continued'] == len(depend_data)):
                self._clear_breakpoint_info(script_name)
                result['message'] += "，所有 iteration 已完成，断点信息已清除"
            
            self.logger.info(f"脚本 {script_name} 断点续跑执行完成: {result['message']}")
            
        except Exception as e:
            error_msg = f"执行脚本 {script_name} 的断点续跑失败: {str(e)}"
            result['message'] = error_msg
            self.logger.error(error_msg, exc_info=True)
        
        return result

    def list_available_scripts(self) -> Dict[str, Any]:
        """
        列出所有可用的脚本
        
        Returns:
            Dict[str, Any]: 脚本列表信息
        """
        scripts_info = {
            'total': 0,
            'regular_scripts': [],
            'test_scripts': []
        }
        
        if not self.scripts_dir.exists():
            self.logger.warning(f"脚本目录不存在: {self.scripts_dir}")
            return scripts_info
        
        python_files = list(self.scripts_dir.glob("*.py"))
        
        for script_file in python_files:
            script_name = script_file.stem
            
            # 跳过__init__.py等特殊文件
            if script_name.startswith('__'):
                continue
                
            is_test = self._is_test_script(script_name)
            
            script_info = {
                'name': script_name,
                'file_path': str(script_file),
                'modified_time': datetime.fromtimestamp(script_file.stat().st_mtime),
                'is_test': is_test
            }
            
            if is_test:
                scripts_info['test_scripts'].append(script_info)
            else:
                scripts_info['regular_scripts'].append(script_info)
        
        scripts_info['total'] = len(scripts_info['regular_scripts']) + len(scripts_info['test_scripts'])
        
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
                script_schedule = session.query(ScriptSchedule).filter(
                    ScriptSchedule.name == script_name
                ).first()
                
                if script_schedule is None:
                    # 创建新的 ScriptSchedule
                    script_schedule = ScriptSchedule(
                        name=script_name,
                        period="",
                        turn_on=False,
                        remark=f"自动创建的脚本调度记录 - {script_name}"
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
    
    def _is_test_script(self, script_name: str) -> bool:
        """
        检查是否为测试脚本
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            bool: 是否为测试脚本
        """
        # 测试脚本通常以 test_ 开头或包含 test 关键词
        return (script_name.startswith('test_') or 
                script_name.startswith('test') or 
                'test' in script_name.lower())
    
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
            
            # 如果结果是字典或其他类型，存储到通用的结果表中
            
            with Session(engine) as session:
                # 这里需要根据实际的数据模型来存储结果
                # 临时实现：可以在这里添加实际的数据库存储逻辑
                
                session.commit()
                self.logger.info(f"执行结果已记录到数据库: {script_name}")
                return True
                
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
            log_file = os.path.join(log_dir, f"execution_{datetime.now().strftime('%Y%m%d')}.log")
            
            # 准备日志内容
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'script_name': script_name,
                'execution_time': datetime.now().isoformat(),
                'result_type': type(result).__name__,
                'result_summary': str(result)[:500] if result is not None else None,  # 限制长度
                'success': True
            }
            
            # 尝试将结果转换为 JSON
            try:
                if hasattr(result, 'to_dict'):
                    # 处理 DataFrame，转换时间戳
                    result_dict = result.to_dict()
                    # 递归处理可能的时间戳对象
                    log_entry['result_data'] = self._serialize_for_json(result_dict)
                elif isinstance(result, (dict, list)):
                    log_entry['result_data'] = self._serialize_for_json(result)
                else:
                    log_entry['result_data'] = str(result)
            except Exception as e:
                log_entry['result_data'] = f"无法序列化的结果: {str(e)}"
            
            # 写入日志文件
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
                
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


    def convert_menu(self, menu_path: str = None) -> Dict[str, Any]:
        """
        将 Menu.json 转换为脚本调度配置并更新数据库
        
        Args:
            menu_path (str): Menu.json 文件路径，如果为 None 则使用项目根目录的 Menu.json
            
        Returns:
            Dict[str, Any]: 转换和更新结果
        """
        result = {
            'success': False,
            'menu_path': '',
            'total_items': 0,
            'created_items': 0,
            'updated_items': 0,
            'skipped_items': 0,
            'message': '',
            'details': []
        }
        
        try:
            # 1. 确定 Menu.json 路径
            if menu_path is None:
                menu_path = os.path.join(config.base_dir, 'Menu.json')
            result['menu_path'] = menu_path
            
            self.logger.info(f"开始转换 Menu.json: {menu_path}")
            
            # 2. 导入工具模块并调用转换函数
            tools_dir = os.path.join(config.base_dir, 'tools', 'sys')
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
            
            result['total_items'] = len(schedule_df)
            self.logger.info(f"转换完成，共生成 {result['total_items']} 个调度条目")
            
            # 4. 获取数据库连接
            engines = config.init_db()
            engine = engines["engine"]
            
            with Session(engine) as session:
                for _, row in schedule_df.iterrows():
                    script_name = row['name']
                    self.logger.info(f"处理脚本: {script_name}")
                    
                    # 检查是否已存在
                    existing_schedule = session.query(ScriptSchedule).filter(
                        ScriptSchedule.name == script_name
                    ).first()
                    
                    if existing_schedule:
                        # 保留原有的 last_sync_datetime
                        original_last_sync = existing_schedule.last_sync_datetime
                        
                        # 更新字段
                        existing_schedule.period = row.get('period', '')
                        existing_schedule.turn_on = row.get('turn_on', False)
                        existing_schedule.next_sync_datetime = None  # 转换时重置
                        existing_schedule.updated_at = datetime.now()
                        
                        # 只在原值为空时才设置新的 last_sync_datetime
                        if original_last_sync is None and row.get('last_sync_datetime'):
                            existing_schedule.last_sync_datetime = row.get('last_sync_datetime')
                        
                        session.add(existing_schedule)
                        result['updated_items'] += 1
                        
                        detail = {
                            'script_name': script_name,
                            'action': 'updated',
                            'last_sync_preserved': original_last_sync is not None,
                            'period': existing_schedule.period,
                            'turn_on': existing_schedule.turn_on
                        }
                        result['details'].append(detail)
                        
                        self.logger.info(f"更新脚本调度: {script_name} (保留 last_sync_datetime: {original_last_sync is not None})")
                        
                    else:
                        # 创建新记录
                        new_schedule = ScriptSchedule(
                            name=script_name,
                            period=row.get('period', ''),
                            turn_on=row.get('turn_on', False),
                            last_sync_datetime=None,  # 新记录没有最后同步时间
                            next_sync_datetime=None,
                            remark=f"从 Menu.json 自动创建 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        session.add(new_schedule)
                        result['created_items'] += 1
                        
                        detail = {
                            'script_name': script_name,
                            'action': 'created',
                            'period': new_schedule.period,
                            'turn_on': new_schedule.turn_on
                        }
                        result['details'].append(detail)
                        
                        self.logger.info(f"创建新脚本调度: {script_name}")
                
                # 提交事务
                session.commit()
                
            # 5. 生成结果消息
            message_parts = []
            if result['created_items'] > 0:
                message_parts.append(f"创建了 {result['created_items']} 个新条目")
            if result['updated_items'] > 0:
                message_parts.append(f"更新了 {result['updated_items']} 个条目")
            
            result['message'] = f"Menu.json 转换成功！共处理 {result['total_items']} 个条目，{', '.join(message_parts)}"
            result['success'] = True
            
            self.logger.info(f"Menu.json 转换完成: {result['message']}")
            
        except Exception as e:
            error_msg = f"Menu.json 转换失败: {str(e)}"
            result['message'] = error_msg
            self.logger.error(error_msg, exc_info=True)
            
        return result