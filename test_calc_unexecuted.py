import datetime
from tools.sys.calcNextSyncDatetime import calcUnExecutedTimes

# 测试函数
def test_calc_unexecuted_times():
    # 获取当前时间
    current_time = datetime.datetime.now()
    print(f"当前时间: {current_time}")
    
    # 测试用例1: 每天执行，步长为0，测试过去3天
    last_sync = current_time - datetime.timedelta(days=3)
    result = calcUnExecutedTimes(last_sync, "every_day", step="0")
    print(f"测试1 - 每天执行，步长0，过去3天: {len(result)}个时间点 {result} (预期: 3个)")
    
    # 测试用例2: 每2天执行，步长为0，测试过去5天
    last_sync = current_time - datetime.timedelta(days=5)
    result = calcUnExecutedTimes(last_sync, "every_day_2", step="0")
    print(f"测试2 - 每2天执行，步长0，过去5天: {len(result)}个时间点 {result} (预期: 2或3个)")
    
    # 测试用例3: 每天执行，步长1小时，测试过去2小时
    last_sync = current_time - datetime.timedelta(hours=2)
    result = calcUnExecutedTimes(last_sync, "every_day", step="1h")
    print(f"测试3 - 每天执行，步长1h，过去2小时: {len(result)}个时间点 {result} (预期: 2个)")
    
    # 测试用例4: 每周一执行，步长为0，测试过去2周
    last_sync = current_time - datetime.timedelta(weeks=2)
    result = calcUnExecutedTimes(last_sync, "every_week_1", step="0")
    print(f"测试4 - 每周一执行，步长0，过去2周: {len(result)}个时间点 {result} (预期: 2个)")
    
    # 测试用例5: 每月1号执行，步长为0，测试过去3个月
    last_sync = current_time - datetime.timedelta(days=90)
    result = calcUnExecutedTimes(last_sync, "every_month_1", step="0")
    print(f"测试5 - 每月1号执行，步长0，过去3个月: {len(result)}个时间点 {result} (预期: 3个)")
    
    # 测试用例6: 直接日期格式，已过去
    last_sync = current_time - datetime.timedelta(days=10)
    target_date = (current_time - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    result = calcUnExecutedTimes(last_sync, target_date, step="0")
    print(f"测试6 - 直接日期格式，已过去: {len(result)}个时间点 {result} (预期: 1个)")
    
    # 测试用例7: 直接日期格式，未到
    last_sync = current_time - datetime.timedelta(days=10)
    target_date = (current_time + datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    result = calcUnExecutedTimes(last_sync, target_date, step="0")
    print(f"测试7 - 直接日期格式，未到: {len(result)}个时间点 {result} (预期: 0个)")
    
    # 测试用例8: 每个工作日执行，步长为0，测试过去7天
    last_sync = current_time - datetime.timedelta(days=7)
    result = calcUnExecutedTimes(last_sync, "every_wDay", step="0")
    print(f"测试8 - 每个工作日执行，步长0，过去7天: {len(result)}个时间点 {result} (预期: 5个)")
    
    # 测试用例9: 每天执行，步长30分钟，测试过去2小时
    last_sync = current_time - datetime.timedelta(hours=2)
    result = calcUnExecutedTimes(last_sync, "every_day", step="30m")
    print(f"测试9 - 每天执行，步长30m，过去2小时: {len(result)}个时间点 {result} (预期: 4个)")
    
    # 测试用例10: 每年3月15号执行，步长为0，测试过去2年
    last_sync = current_time - datetime.timedelta(days=730)
    result = calcUnExecutedTimes(last_sync, "every_month3_15", step="0")
    print(f"测试10 - 每年3月15号执行，步长0，过去2年: {len(result)}个时间点 {result} (预期: 2个)")

if __name__ == "__main__":
    test_calc_unexecuted_times()