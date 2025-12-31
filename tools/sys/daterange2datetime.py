# 写一个函数,用来转换字符串到时间范围
# 例如:
# 输入: "5Y"
# 输出: "2020-12-12,2025-12-12"
# 输入: "5M"
# 输出: "2024-01-12,2024-06-12"
# 输入: "5D"
# 输出: "2024-06-12,2024-06-17"
# 输入: "5H"
# 输出: "2024-06-17 00:00:00,2024-06-17 05:00:00"

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re


def str_to_time_range(time_str):
    """
    将字符串时间范围转换为具体时间区间
    
    参数:
        time_str: str, 时间范围字符串，如"5Y", "5M", "5D", "5H"
    
    返回:
        str: 格式化的时间范围字符串，如"2020-12-12,2025-12-12"
    """
    # 验证输入格式
    pattern = r'^(\d+)([YMDH])$'
    match = re.match(pattern, time_str)
    if not match:
        return ""
    
    # 提取数字和时间单位
    num = int(match.group(1))
    unit = match.group(2)
    
    # 获取当前时间作为结束时间
    end_date = datetime.now()
    
    # 根据时间单位计算开始时间
    if unit == 'Y':  # 年
        start_date = end_date - relativedelta(years=num)
        format_str = "%Y-%m-%d"
    elif unit == 'M':  # 月
        start_date = end_date - relativedelta(months=num)
        format_str = "%Y-%m-%d"
    elif unit == 'D':  # 日
        start_date = end_date - timedelta(days=num)
        format_str = "%Y-%m-%d"
    elif unit == 'H':  # 小时
        # 对于小时，结束时间设为当天0点，开始时间为结束时间减去指定小时数
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(hours=num)
        format_str = "%Y-%m-%d %H:%M:%S"
    else:
        return ""
    
    # 格式化输出
    return f"{start_date.strftime(format_str)},{end_date.strftime(format_str)}"

# 测试示例
if __name__ == "__main__":
    print(str_to_time_range("5Y"))  # 输出类似: 2020-12-12,2025-12-12
    print(str_to_time_range("5M"))  # 输出类似: 2024-01-12,2024-06-12
    print(str_to_time_range("5D"))  # 输出类似: 2024-06-12,2024-06-17
    print(str_to_time_range("5H"))  # 输出类似: 2024-06-17 00:00:00,2024-06-17 05:00:00
    print(str_to_time_range("10Y")) # 输出类似: 2015-12-12,2025-12-12
    print(str_to_time_range("3M"))  # 输出类似: 2024-03-12,2024-06-12