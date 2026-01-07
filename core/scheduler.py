# 写一个类，用于管理脚本的调度
# 类名：ScriptScheduler
# 功能：管理脚本的调度
# 方法：
#     __init__(self, max_workers: int = 5)
#     start(self)  # 启动调度器, 开始调度脚本, 会调用self._load_scripts加载所有脚本的调度信息,以及_immediate_execute执行所有immediate为True的脚本
#     stop(self, immediate: bool = False)  # 停止调度器, 结束所有脚本的执行,有两种模式: 1. 立即结束所有脚本的执行 2. 等待当前脚本执行完毕后结束
#     _immediate_execute(self, script_name: str)  # 为所有schedule信息中有immediate为True的脚本调用self._execute_script
#     _load_scripts(self)  # 从数据库加载所有脚本的调度信息，计算下次执行时间并加入调度堆
#     _execute_script(self, script_name: str)  # 触发脚本的执行，使用config获取对应脚本的基本信息,将调度信息添加到基本信息的schedule字段，执行handler中的_execute_script方法，执行完毕后更新脚本调度表的last_sync_datetime字段
#     scheduler_loop(self)  # 从调度堆中取出下一个要执行的脚本，调用self._execute_script，执行完毕后计算下次执行时间，更新脚本调度表后，并加入调度堆
#     carry_up(self, script_name: str)  # 手动触发脚本的执行，会根据脚本的最后执行时间计算出一个执行时间在当前时间之前的列表，循环调用self._execute_script


import logging
import threading
import time
from datetime import datetime
from heapq import heappush, heappop
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlmodel import Session
from typing import Dict, Any, List, Optional

from .config import config
from .models import ScriptSyncSchedule, ScriptSyncMenu
from .handler import ScriptHandler
from tools.sys.calcNextSyncDatetime import calcNextSyncDatetime, calcUnExecutedTimes


class ScriptScheduler:
    """
    脚本调度器类
    负责管理脚本的调度，包括启动、停止、立即执行、定时执行等功能
    """

    def __init__(self, max_workers: int = 5):
        """
        初始化脚本调度器

        Args:
            max_workers (int): 线程池最大线程数，默认为5
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.scheduler_thread = None
        self.is_running = False
        self.stop_event = threading.Event()
        self.schedule_heap = []  # 调度堆，存储(下次执行时间, 脚本名称)
        self.logger = self._setup_logger()
        self.handler = ScriptHandler()

    def _setup_logger(self) -> logging.Logger:
        """
        设置日志记录器

        Returns:
            logging.Logger: 配置好的日志记录器
        """
        logger = logging.getLogger("script_scheduler")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def start(self):
        """
        启动调度器，开始调度脚本
        会调用self._load_scripts加载所有脚本的调度信息，以及_immediate_execute执行所有immediate为True的脚本
        """
        try:
            self.logger.info("启动脚本调度器...")
            self.is_running = True
            self.stop_event.clear()

            # 加载所有脚本的调度信息
            self._load_scripts()

            # 执行所有immediate为True的脚本
            self._immediate_execute()

            # 启动调度循环线程
            self.scheduler_thread = threading.Thread(target=self.scheduler_loop, daemon=True)
            self.scheduler_thread.start()

            self.logger.info("脚本调度器启动成功")
        except Exception as e:
            self.logger.error(f"启动脚本调度器失败: {str(e)}")
            self.is_running = False

    def stop(self, immediate: bool = False):
        """
        停止调度器，结束所有脚本的执行

        Args:
            immediate (bool): 是否立即结束所有脚本的执行
                - True: 立即结束所有脚本的执行
                - False: 等待当前脚本执行完毕后结束
        """
        try:
            self.logger.info(f"停止脚本调度器，立即模式: {immediate}")
            self.is_running = False
            self.stop_event.set()

            if immediate:
                # 立即结束所有线程，不推荐使用，可能导致资源泄漏
                self.executor.shutdown(wait=False)
                self.logger.info("脚本调度器已立即停止")
            else:
                # 等待当前任务完成后结束
                self.executor.shutdown(wait=True)
                self.logger.info("脚本调度器已正常停止")

            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
                if self.scheduler_thread.is_alive():
                    self.logger.warning("调度线程未能在超时时间内结束")

        except Exception as e:
            self.logger.error(f"停止脚本调度器失败: {str(e)}")

    def _immediate_execute(self):
        """
        为所有schedule信息中有immediate为True的脚本调用self._execute_script
        """
        try:
            self.logger.info("执行所有需要立即执行的脚本...")
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                # 查询所有immediate为True的脚本
                immediate_scripts = session.query(ScriptSyncSchedule).filter(
                    ScriptSyncSchedule.immediate == True
                ).all()

            for script in immediate_scripts:
                self.logger.info(f"立即执行脚本: {script.name}")
                self._execute_script(script.name)

        except Exception as e:
            self.logger.error(f"执行立即脚本失败: {str(e)}")

    def _load_scripts(self):
        """
        从数据库加载所有脚本的调度信息，计算下次执行时间并加入调度堆
        """
        try:
            self.logger.info("加载所有脚本的调度信息...")
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                # 查询所有脚本的调度信息
                scripts_schedule = session.query(ScriptSyncSchedule).filter(
                    ScriptSyncSchedule.turn_on == True and
                    ScriptSyncSchedule.period != None
                ).all()
            # 清空调度堆
            self.schedule_heap = []

            for script in scripts_schedule:
                if script.period:
                    print(script)
                    # 计算下次执行时间
                    last_sync = script.last_sync_datetime or datetime.now()
                    # 确保参数符合函数要求
                    start_time = script.start_time or "00:00:00"
                    end_time = script.end_time or start_time or "23:59:59"
                    step = script.step or "0"
                    next_sync = calcNextSyncDatetime(
                                current_datetime=last_sync, 
                                period=script.period,
                                start_time=start_time,
                                end_time=end_time,
                                step=step
                        )
                    if next_sync:
                        # 加入调度堆
                        heappush(self.schedule_heap, (next_sync, script.name))
                        self.logger.info(f"脚本 {script.name} 已加入调度堆，下次执行时间: {next_sync}")

            self.logger.info(f"共加载 {len(self.schedule_heap)} 个脚本到调度堆")

        except Exception as e:
            self.logger.error(f"加载脚本调度信息失败: {str(e)}")

    def _execute_script(self, script_name: str):
        """
        触发脚本的执行
        使用config获取对应脚本的基本信息，将调度信息添加到基本信息的schedule字段
        执行handler中的_execute_script方法
        执行完毕后更新脚本调度表的last_sync_datetime字段

        Args:
            script_name (str): 脚本名称
        """
        try:
            self.logger.info(f"开始执行脚本: {script_name}")
            engines = config.init_db()
            engine = engines["engine"]

            # 查询脚本调度信息
            with Session(engine) as session:
                script_schedule = session.query(ScriptSyncSchedule).filter(
                    ScriptSyncSchedule.name == script_name
                ).first()
                script_menu = session.query(ScriptSyncMenu).filter(
                    ScriptSyncMenu.name == script_name
                ).first()
                if not script_schedule or not script_menu:
                    self.logger.error(f"脚本 {script_name} 的调度信息或菜单不存在")
                    return

            # 使用线程池执行脚本
            future = self.executor.submit(self._run_script_with_handler, script_schedule, script_menu)
            future.add_done_callback(lambda f: self._update_script_last_sync(f, script_schedule.name))

        except Exception as e:
            self.logger.error(f"触发脚本执行失败 {script_name}: {str(e)}")

    def _run_script_with_handler(self, script_schedule: ScriptSyncSchedule, script_menu: ScriptSyncMenu) -> Dict[str, Any]:
        """
        使用handler执行脚本

        Args:
            script_schedule: 脚本调度信息
            script_menu: 脚本菜单信息

        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            print(f"script_schedule: {script_schedule}")
            func_name = script_schedule.func_name
            if not func_name:
                func_name = 'period' if script_menu.type == 'single' else 'iteration'
            # 执行脚本，默认调用period函数
            result = self.handler._execute_script(
                script_name=script_menu.name,
                func_name=func_name,
                type=script_menu.type,
                save_to_db=script_menu.save_to_db,
                interval=script_menu.interval,
                is_error_stop=script_menu.is_error_stop,
            )
            return result
        except Exception as e:
            self.logger.error(f"执行脚本 {script_schedule.name} 失败: {str(e)}")
            return {"success": False, "message": str(e)}

    def _update_script_last_sync(self, future, script_name: str):
        """
        更新脚本的最后执行时间

        Args:
            future: 线程池执行结果
            script_name (str): 脚本名称
        """
        try:
            # 获取执行结果
            result = future.result()
            self.logger.info(f"脚本 {script_name} 执行完成，结果: {'成功' if result.get('success') else '失败'}")

            # 更新数据库中的最后执行时间
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                script_schedule = session.query(ScriptSyncSchedule).filter(
                    ScriptSyncSchedule.name == script_name
                ).first()

                if script_schedule:
                    script_schedule.last_sync_datetime = datetime.now()
                    script_schedule.immediate = False  # 执行后重置immediate标志
                    session.add(script_schedule)
                    session.commit()

                    # 重新计算下次执行时间并加入调度堆
                    if script_schedule.period and self.is_running:
                        # 确保所有参数都有合理的默认值
                        start_time = script_schedule.start_time or "00:00:00"
                        end_time = script_schedule.end_time or start_time or "23:59:59"
                        step = script_schedule.step or "0"
                        next_sync = calcNextSyncDatetime(
                                current_datetime=datetime.now(), 
                                period=script_schedule.period,
                                start_time=start_time,
                                end_time=end_time,
                                step=step
                        )
                        if next_sync:
                            heappush(self.schedule_heap, (next_sync, script_name))
                            self.logger.info(f"脚本 {script_name} 已重新加入调度堆，下次执行时间: {next_sync}")

        except Exception as e:
            self.logger.error(f"更新脚本 {script_name} 最后执行时间失败: {str(e)}")

    def scheduler_loop(self):
        """
        调度循环
        从调度堆中取出下一个要执行的脚本，调用self._execute_script
        执行完毕后计算下次执行时间，更新脚本调度表后，并加入调度堆
        """
        self.logger.info("调度循环已启动")

        while self.is_running and not self.stop_event.is_set():
            try:
                if not self.schedule_heap:
                    # 调度堆为空，等待一段时间后重新加载脚本
                    self.logger.info("调度堆为空，等待5秒后重新加载脚本")
                    time.sleep(5)
                    self._load_scripts()
                    continue

                # 获取下一个要执行的脚本
                next_exec_time, script_name = heappop(self.schedule_heap)
                now = datetime.now()

                if next_exec_time > now:
                    # 计算需要等待的时间
                    wait_time = (next_exec_time - now).total_seconds()
                    self.logger.info(f"等待 {wait_time:.2f} 秒后执行脚本: {script_name}")

                    # 等待直到到达执行时间或停止信号
                    if self.stop_event.wait(timeout=wait_time):
                        # 收到停止信号，退出循环
                        break

                # 执行脚本
                self._execute_script(script_name)

            except Exception as e:
                self.logger.error(f"调度循环执行失败: {str(e)}")
                # 防止异常导致循环退出
                time.sleep(1)

        self.logger.info("调度循环已结束")

    def carry_up(self, script_name: str):
        """
        手动触发脚本的执行
        会根据脚本的最后执行时间计算出一个执行时间在当前时间之前的列表
        循环调用self._execute_script

        Args:
            script_name (str): 脚本名称
        """
        try:
            self.logger.info(f"手动触发脚本执行: {script_name}")
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                script_schedule = session.query(ScriptSyncSchedule).filter(
                    ScriptSyncSchedule.name == script_name
                ).first()

                if not script_schedule:
                    self.logger.error(f"脚本 {script_name} 的调度信息不存在")
                    return

                if not script_schedule.period:
                    self.logger.error(f"脚本 {script_name} 没有设置执行周期")
                    return

                # 计算需要补执行的次数
                last_sync = script_schedule.last_sync_datetime or datetime.now()
                now = datetime.now()
                exec_times = []

                # 计算所有应该执行但未执行的时间点
                exec_times = calcUnExecutedTimes(
                    last_sync,
                    script_schedule.period,
                    script_schedule.start_time or "00:00:00",
                    script_schedule.end_time or "23:59:59",
                    script_schedule.step or "0"
                )

            if not exec_times:
                self.logger.info(f"脚本 {script_name} 没有需要补执行的任务")
                return

            self.logger.info(f"脚本 {script_name} 需要补执行 {len(exec_times)} 次")

            # 循环执行脚本
            for exec_time in exec_times:
                self.logger.info(f"补执行脚本 {script_name}，计划执行时间: {exec_time}")
                self._execute_script(script_name)
                # 每次执行后等待1秒，避免过于密集
                time.sleep(1)

        except Exception as e:
            self.logger.error(f"手动触发脚本执行失败 {script_name}: {str(e)}")

    def print_schedule_heap(self):
        """
        打印当前调度堆中的所有任务
        """
        self._load_scripts()
        self.logger.info("当前调度堆中的任务:")
        for next_exec_time, script_name in self.schedule_heap:
            print(f"📝脚本 {script_name} 下次执行时间: {next_exec_time}")
