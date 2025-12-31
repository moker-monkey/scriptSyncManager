#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本：验证计算下次执行时间的功能
"""

import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append('/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager')

from core.handler import ScriptHandler

def test_calculate_next_sync_time():
    """
    测试计算下次执行时间的功能
    """
    print("开始测试计算下次执行时间的功能...")
    print("=" * 50)
    
    handler = ScriptHandler()
    current_time = datetime.now()
    
    # 测试用例：不同的crontab表达式
    test_cases = [
        # crontab表达式, 描述
        ("0 */6 * * *", "每6小时执行一次"),
        ("0 0 * * *", "每天午夜执行"),
        ("0 9 * * 1-5", "工作日早上9点执行"),
        ("*/30 * * * *", "每30分钟执行一次"),
        ("30 14 * * 1,3,5", "每周一、三、五下午2:30执行"),
    ]
    
    all_passed = True
    
    for cron_expr, description in test_cases:
        print(f"\n测试: {description}")
        print(f"crontab表达式: {cron_expr}")
        print(f"当前时间: {current_time}")
        
        try:
            next_time = handler._calculate_next_sync_time(current_time, cron_expr)
            if next_time:
                print(f"下次执行时间: {next_time}")
                print(f"时间差: {next_time - current_time}")
                # 验证下次时间是否在当前时间之后
                if next_time > current_time:
                    print("✅ 测试通过：下次执行时间正确")
                else:
                    print("❌ 测试失败：下次执行时间应该在当前时间之后")
                    all_passed = False
            else:
                print("❌ 测试失败：未能计算出下次执行时间")
                all_passed = False
        except Exception as e:
            print(f"❌ 测试失败：计算过程中发生错误: {str(e)}")
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("所有测试通过!")
        return True
    else:
        print("测试失败!")
        return False

if __name__ == "__main__":
    success = test_calculate_next_sync_time()
    sys.exit(0 if success else 1)