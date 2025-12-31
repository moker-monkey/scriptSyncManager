# 自定义period表达式字符串，支持以下格式：
# - "every_day_6"：每天6点
# - "every_day_6-14"：每天6点到14点，超出范围则次日执行
# - "every_WDay_6"：每个工作日6点
# - "every_week1_6"：每周一(1)6点
# - "every_week1_6-14"：每周一6点到14点
# - "every_month_3_6"：每月3号6点
# - "every_month3_L_6"：每年3月的最后一天6点
# - "every_day_6_period_minute5"：每5分钟执行一次，每天6点都执行
# - "every_day_6_period_sec10"：每10秒执行一次，每天6点都执行
# - "every_day_6_period_hour1"：每1小时执行一次，每天6点都执行
# - "every_day6_6_period_hour1"：每1小时执行一次，每隔6天的6点执行一次
# - "period_minute5"：每5分钟执行一次
# - "period_sec10"：每10秒执行一次
# - "period_hour1"：每1小时执行一次
# - 直接时间格式："2024-01-01 00:00:00"直接返回该时间

from datetime import datetime, timedelta
import calendar
import re

def parse_hour(hour_str):
    """解析小时字符串，支持范围格式(如"6-14")，返回开始小时和结束小时"""
    if '-' in hour_str:
        start, end = map(int, hour_str.split('-'))
        return start, end
    return int(hour_str), None

def get_fixed_interval(last_sync, interval_type, interval_value):
    """计算固定间隔的下一次执行时间"""
    intervals = {
        'minute': timedelta(minutes=interval_value),
        'sec': timedelta(seconds=interval_value),  # 注意：应该是second，但保持原格式
        'hour': timedelta(hours=interval_value)
    }
    return last_sync + intervals.get(interval_type, timedelta())

def get_next_day_time(last_sync, hour, end_hour=None, days_interval=1):
    """计算每天的下一次执行时间"""
    next_sync = datetime(last_sync.year, last_sync.month, last_sync.day, hour, 0, 0)
    
    # 如果今天的指定时间已过
    if next_sync <= last_sync:
        # 如果有结束时间，检查是否在范围内
        if end_hour and last_sync.hour < end_hour:
            next_sync = last_sync.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            # 添加天数间隔
            next_sync += timedelta(days=days_interval)
    else:
        # 如果当前时间还没到指定时间，但需要考虑天数间隔
        # 检查是否需要跳过几天
        days_since_start = (last_sync - next_sync).days % days_interval
        if days_since_start != 0:
            next_sync += timedelta(days=days_interval - days_since_start)
    
    return next_sync

def get_next_weekday_time(last_sync, weekday, hour, end_hour=None):
    """计算每周指定星期几的下一次执行时间(1-7表示周一到周日)"""
    # 计算距离下一个指定星期几的天数
    days_ahead = weekday - last_sync.isoweekday()
    if days_ahead <= 0:
        days_ahead += 7
    
    next_sync = last_sync + timedelta(days=days_ahead)
    next_sync = datetime(next_sync.year, next_sync.month, next_sync.day, hour, 0, 0)
    
    # 检查是否今天就是指定星期几但时间已过
    if last_sync.isoweekday() == weekday:
        today_start = datetime(last_sync.year, last_sync.month, last_sync.day, hour, 0, 0)
        if today_start <= last_sync:
            # 如果有结束时间，检查是否在范围内
            if end_hour and last_sync.hour < end_hour:
                next_sync = last_sync.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_sync += timedelta(weeks=1)
    
    return next_sync

def get_next_month_time(last_sync, month, day_info, hour, end_hour=None):
    """计算每月的下一次执行时间"""
    # 确定下一个年份和月份
    next_year = last_sync.year
    next_month = month
    
    # 检查当前是否是目标月份
    if last_sync.month > month or (last_sync.month == month and (day_info != 'L' and int(day_info) < last_sync.day)):
        next_year += 1
    
    # 如果今天就是目标日期，需要特殊处理
    if last_sync.month == month:
        # 计算目标日期
        if day_info == 'L':
            target_day = calendar.monthrange(last_sync.year, month)[1]
        else:
            target_day = int(day_info)
        
        # 如果今天就是目标日期
        if last_sync.day == target_day:
            # 检查时间是否在小时范围内
            if end_hour and hour <= last_sync.hour < end_hour:
                # 在小时范围内，下一次执行时间是当前时间的下一个小时
                return last_sync.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            elif last_sync.hour >= end_hour:
                # 超过结束时间，下一次执行时间是下一年的目标日期
                next_year += 1
    
    # 处理每月最后一天的情况
    if day_info == 'L':
        last_day = calendar.monthrange(next_year, next_month)[1]
        return datetime(next_year, next_month, last_day, hour, 0, 0)
    else:
        # 处理指定日期的情况
        day = int(day_info)
        max_day = calendar.monthrange(next_year, next_month)[1]
        if day > max_day:
            day = max_day
        return datetime(next_year, next_month, day, hour, end_hour)

def get_next_workday_time(last_sync, hour, end_hour=None):
    """计算下一个工作日的执行时间"""
    # 尝试在接下来的7天内找到下一个工作日
    for i in range(7):
        next_day = last_sync + timedelta(days=i)
        if 1 <= next_day.isoweekday() <= 5:  # 1-5表示周一到周五
            next_sync = datetime(next_day.year, next_day.month, next_day.day, hour, 0, 0)
            
            # 如果这是当前日期
            if i == 0:
                # 如果当前时间在小时范围内且已过开始时间
                if end_hour and hour <= last_sync.hour < end_hour:
                    return last_sync.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                # 如果已过结束时间
                elif end_hour and last_sync.hour >= end_hour:
                    continue
                # 如果还没到开始时间
                elif next_sync > last_sync:
                    break
            # 如果是未来日期
            else:
                break
    return next_sync

def calc_next_sync_datetime(period, last_sync_datetime=None):
    """
    根据周期计算下一次同步的日期时间
    
    参数:
        period: str, 周期表达式
        last_sync_datetime: str, 上次同步的日期字符串，格式为"YYYY-MM-DD HH:MM:SS"
    返回:
        str: 下一次同步的时间字符串，格式为"YYYY-MM-DD HH:MM:SS"
    """
    try:
        # 解析上次同步日期
        last_sync = datetime.now() if not last_sync_datetime else datetime.strptime(last_sync_datetime, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return ""
    
    # 直接时间格式处理
    if ' ' in period:
        try:
            return datetime.strptime(period, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""
    
    # 解析period表达式
    parts = period.split("_")
    
    # period_xxx格式处理
    if parts[0] == "period":
        if len(parts) != 2:
            return ""
        try:
            interval_type, interval_value = re.match(r'([a-zA-Z]+)(\d+)', parts[1]).groups()
            next_sync = get_fixed_interval(last_sync, interval_type, int(interval_value))
            return next_sync.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError):
            return ""
    
    # every_xxx格式处理
    if parts[0] == "every":
        try:
            # 检查是否包含period
            period_index = next((i for i, part in enumerate(parts) if part == "period"), None)
            base_parts = parts[1:period_index] if period_index else parts[1:]
            
            # 解析基础时间
            if base_parts[0] == "day":
                hour, end_hour = parse_hour(base_parts[1])
                next_sync = get_next_day_time(last_sync, hour, end_hour)
            elif base_parts[0].startswith("day") and len(base_parts[0]) > 3:
                # 处理格式：every_day6_6 (每隔6天6点)
                days_interval = int(base_parts[0][3:])
                hour, end_hour = parse_hour(base_parts[1])
                next_sync = get_next_day_time(last_sync, hour, end_hour, days_interval)
            
            elif base_parts[0] == "WDay":
                hour, end_hour = parse_hour(base_parts[1])
                next_sync = get_next_workday_time(last_sync, hour, end_hour)
            
            elif base_parts[0].startswith("week"):
                weekday = int(base_parts[0][4:])
                hour, end_hour = parse_hour(base_parts[1])
                next_sync = get_next_weekday_time(last_sync, weekday, hour, end_hour)
            
            elif base_parts[0].startswith("month"):
                # 根据base_parts[0]的格式区分两种月份格式
                if base_parts[0] == "month":
                    # 格式：every_month_3_6
                    if len(base_parts) < 3:
                        return ""
                    month = int(base_parts[1])
                    hour, end_hour = parse_hour(base_parts[2])
                    next_sync = get_next_month_time(last_sync, month, base_parts[1], hour, end_hour)
                else:
                    # 格式：every_month3_L_6
                    if len(base_parts) < 3:
                        return ""
                    month = int(base_parts[0][5:])
                    hour, end_hour = parse_hour(base_parts[2])
                    next_sync = get_next_month_time(last_sync, month, base_parts[1], hour, end_hour)
            
            else:
                return ""
            
            # 处理period部分
            if period_index and len(parts) > period_index + 1:
                interval_type, interval_value = re.match(r'([a-zA-Z]+)(\d+)', parts[period_index + 1]).groups()
                interval_value = int(interval_value)
                
                # 确保至少应用一次时间间隔，即使next_sync等于last_sync
                if next_sync <= last_sync:
                    # 如果next_sync小于等于last_sync，需要应用时间间隔直到超过last_sync
                    while next_sync <= last_sync:
                        next_sync = get_fixed_interval(next_sync, interval_type, interval_value)
                else:
                    # 如果next_sync大于last_sync，也需要应用一次时间间隔（因为period表示的是执行间隔）
                    next_sync = get_fixed_interval(next_sync, interval_type, interval_value)
            
            return next_sync.strftime("%Y-%m-%d %H:%M:%S")
            
        except (ValueError, IndexError, AttributeError) as e:
            print(f"Error: {e}")  # 调试信息
            return ""
    
    return ""

# 测试代码
if __name__ == "__main__":
    # 测试每天的情况
    # print("every_day_6:", calc_next_sync_datetime("every_day_6"))
    # print("every_day_6-14:", calc_next_sync_datetime("every_day_6-14", "2025-12-01 13:00:00"))
    
    # 测试每周的情况
    # print("every_week1_6:", calc_next_sync_datetime("every_week1_6", "2025-12-01 00:00:00"))
    # print("every_week1_6-14:", calc_next_sync_datetime("every_week1_6-14", "2025-12-01 14:00:00"))
    
    # # 测试每月的情况
    # print("every_month_3_6:", calc_next_sync_datetime("every_month_3_6", "2025-12-01 00:00:00"))
    # print("every_month3_3_6_period_hour1:", calc_next_sync_datetime("every_month3_3_6_period_hour1", "2024-03-31 09:00:00"))
    print("every_month12_L_0:", calc_next_sync_datetime("every_month12_L_0", "2025-03-31 06:00:00"))
    
    # # 测试工作日的情况
    # print("every_WDay_6:", calc_next_sync_datetime("every_WDay_6", "2025-12-01 00:00:00"))
    
    # # 测试结合固定间隔的情况
    # print("every_day_6_period_minute5:", calc_next_sync_datetime("every_day_6_period_minute5", "2025-12-01 23:59:59"))
    # print("every_day_6_period_sec10:", calc_next_sync_datetime("every_day_6_period_sec10", "2025-12-01 23:59:59"))
    # print("every_day_6_period_hour1:", calc_next_sync_datetime("every_day_6_period_hour1", "2025-12-01 23:59:59"))
    # print("every_day6_6_period_hour1:", calc_next_sync_datetime("every_day6_6_period_hour1", "2025-12-01 23:59:59"))
    
    # # 测试固定间隔的情况
    # print("period_minute5:", calc_next_sync_datetime("period_minute5", "2025-12-01 00:00:00"))
    # print("period_sec10:", calc_next_sync_datetime("period_sec10", "2025-12-01 00:00:00"))
    # print("period_hour1:", calc_next_sync_datetime("period_hour1", "2025-12-01 00:00:00"))
    
    # # 测试直接时间格式
    # print("2024-01-01 00:00:00:", calc_next_sync_datetime("2024-01-01 00:00:00"))
    
    # # 测试格式不正确的情况
    # print("invalid_format:", calc_next_sync_datetime("invalid_format", "2025-12-01 00:00:00"))