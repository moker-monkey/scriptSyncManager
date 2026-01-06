import datetime
import re

# 计算下一次同步时间
# period的参数说明
# - "every_day"：每天
# - "every_day_6": 每6天
# - "every_wDay"：每个工作日
# - "every_week_1"：每周一
# - "every_month_3"：每月3号
# - "every_month3_L"：每年3月的最后一天
# - "every_month3_15"：每年3月15号
# - 直接时间格式："2024-01-01"直接返回该日期

# - step: 在执行时间内的步长，最大单位为小时，最小单位为秒，例如：1h,1m,10s,特殊单位为0表示在开始时间执行一次，默认0
def calcNextSyncDatetime(
    current_datetime: datetime.datetime,
    period: str,
    start_time: str = "00:00:00",
    end_time: str = "23:59:59",
    step: str = "0"
) -> datetime.datetime:
    """
    计算下一次同步时间

    :param current_datetime: 当前时间
    :param period: 同步周期
    :param start_time: 同步开始时间
    :param end_time: 同步结束时间
    :param step: 同步时间步长
    :return: 下一次同步时间
    """
    # 解析开始时间和结束时间
    start_hour, start_minute, start_second = map(int, start_time.split(":"))
    end_hour, end_minute, end_second = map(int, end_time.split(":"))
    
    # 解析步长字符串为秒数
    def parse_step(step_str: str) -> int:
        """解析步长字符串为秒数"""
        if step_str == "0":
            return 0
        match = re.match(r"(\d+)([hms])", step_str)
        if not match:
            raise ValueError(f"Invalid step format: {step_str}")
        value = int(match.group(1))
        unit = match.group(2)
        if unit == "h":
            return value * 3600
        elif unit == "m":
            return value * 60
        else:  # "s"
            return value
    
    step_seconds = parse_step(step)
    
    # 处理直接时间格式
    if re.match(r"\d{4}-\d{2}-\d{2}", period):
        target_date = datetime.datetime.strptime(period, "%Y-%m-%d")
        target_datetime = target_date.replace(
            hour=start_hour, minute=start_minute, second=start_second, microsecond=0
        )
        return target_datetime
    
    # 处理各种周期格式
    def get_next_base_date() -> datetime.datetime:
        """根据周期获取下一次执行的基础日期（仅包含年月日）"""
        if period == "every_day":
            # 每天执行
            next_date = current_datetime + datetime.timedelta(days=1)
            return next_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif period.startswith("every_day_"):
            # 每n天执行
            days = int(period.split("_")[-1])
            next_date = current_datetime + datetime.timedelta(days=days)
            return next_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif period == "every_wDay":
            # 每个工作日执行
            next_date = current_datetime + datetime.timedelta(days=1)
            while next_date.weekday() >= 5:  # 0-4是工作日，5-6是周末
                next_date += datetime.timedelta(days=1)
            return next_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif period.startswith("every_week_"):
            # 每周几执行
            target_weekday = int(period.split("_")[-1])  # 1-7对应周一到周日
            current_weekday = current_datetime.isoweekday()  # 1-7
            days_ahead = target_weekday - current_weekday
            if days_ahead <= 0:
                days_ahead += 7
            next_date = current_datetime + datetime.timedelta(days=days_ahead)
            return next_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif period.startswith("every_month_"):
            # 每月几号执行
            day = int(period.split("_")[-1])
            # 计算下一个月或本月
            if current_datetime.day < day:
                # 本月执行
                next_date = current_datetime.replace(day=day, hour=0, minute=0, second=0, microsecond=0)
            else:
                # 下月执行
                next_month = current_datetime.month + 1
                next_year = current_datetime.year
                if next_month > 12:
                    next_month = 1
                    next_year += 1
                # 处理月份天数问题
                try:
                    next_date = current_datetime.replace(year=next_year, month=next_month, day=day, 
                                                      hour=0, minute=0, second=0, microsecond=0)
                except ValueError:
                    # 如果该月没有这么多天，使用该月最后一天
                    next_date = current_datetime.replace(year=next_year, month=next_month, day=1, 
                                                      hour=0, minute=0, second=0, microsecond=0)
                    next_date += datetime.timedelta(days=32)
                    next_date = next_date.replace(day=1) - datetime.timedelta(days=1)
            return next_date
        
        elif period.endswith("_L"):
            # 每月最后一天执行，格式如every_month3_L表示每年3月的最后一天
            # 提取月份数字，处理every_month3_L格式
            match = re.match(r"every_month(\d+)_L", period)
            if not match:
                raise ValueError(f"Invalid month_L format: {period}")
            month = int(match.group(1))
            # 计算下一个执行年份和月份
            if current_datetime.month < month or (current_datetime.month == month and current_datetime.day < 28):
                # 今年该月执行
                next_year = current_datetime.year
                next_month = month
            else:
                # 明年该月执行
                next_year = current_datetime.year + 1
                next_month = month
            # 获取该月最后一天
            next_date = datetime.datetime(next_year, next_month, 1)
            next_date += datetime.timedelta(days=32)
            next_date = next_date.replace(day=1) - datetime.timedelta(days=1)
            return next_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        elif re.match(r"every_month(\d+)_(\d+)", period):
            # 每年固定月份的固定日期执行，格式如every_month3_15表示每年3月15号
            match = re.match(r"every_month(\d+)_(\d+)", period)
            if not match:
                raise ValueError(f"Invalid month_day format: {period}")
            month = int(match.group(1))
            day = int(match.group(2))
            
            # 计算下一个执行年份和月份
            if current_datetime.month < month or (current_datetime.month == month and current_datetime.day < day):
                # 今年该月执行
                next_year = current_datetime.year
                next_month = month
            else:
                # 明年该月执行
                next_year = current_datetime.year + 1
                next_month = month
            
            # 构造执行日期
            try:
                next_date = datetime.datetime(next_year, next_month, day, 0, 0, 0, 0)
            except ValueError:
                # 如果该月没有这么多天，使用该月最后一天
                next_date = datetime.datetime(next_year, next_month, 1, 0, 0, 0, 0)
                next_date += datetime.timedelta(days=32)
                next_date = next_date.replace(day=1) - datetime.timedelta(days=1)
            
            return next_date
        
        else:
            raise ValueError(f"Invalid period format: {period}")
    
    # 计算当天的开始和结束时间
    def calculate_day_datetimes(base_date: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
        """计算指定日期的开始和结束时间"""
        start = base_date.replace(hour=start_hour, minute=start_minute, second=start_second, microsecond=0)
        end = base_date.replace(hour=end_hour, minute=end_minute, second=end_second, microsecond=0)
        return start, end
    
    # 获取当前日期的开始和结束时间
    today = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start, today_end = calculate_day_datetimes(today)
    
    # 处理步长为0的情况（只在开始时间执行一次）
    if step_seconds == 0:
        # 检查今天的开始时间是否已过
        if current_datetime < today_start:
            return today_start
        else:
            # 返回下一次执行日期的开始时间
            next_base_date = get_next_base_date()
            next_start, _ = calculate_day_datetimes(next_base_date)
            return next_start
    
    # 处理有步长的情况
    else:
        # 生成今天的所有执行时间点
        def generate_exec_times(start_dt: datetime.datetime, end_dt: datetime.datetime, step_sec: int) -> list[datetime.datetime]:
            """生成指定时间范围内的所有执行时间点"""
            exec_times = []
            current = start_dt
            while current <= end_dt:
                exec_times.append(current)
                current += datetime.timedelta(seconds=step_sec)
            return exec_times
        
        # 检查今天的执行时间点
        today_exec_times = generate_exec_times(today_start, today_end, step_seconds)
        for exec_time in today_exec_times:
            if exec_time > current_datetime:
                return exec_time
        
        # 如果今天没有剩余执行时间点，返回下一次执行日期的第一个执行时间点
        next_base_date = get_next_base_date()
        next_start, next_end = calculate_day_datetimes(next_base_date)
        next_exec_times = generate_exec_times(next_start, next_end, step_seconds)
        return next_exec_times[0]