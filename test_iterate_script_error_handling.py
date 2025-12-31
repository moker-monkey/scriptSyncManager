#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本：验证iterate_script函数的错误处理逻辑
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append('/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager')

from core.handler import ScriptHandler

def test_iterate_script_error_handling():
    """
    测试iterate_script函数的错误处理逻辑
    """
    print("开始测试iterate_script函数的错误处理逻辑...")
    print("=" * 60)
    
    # 创建一个临时测试脚本
    test_script_content = '''
from datetime import datetime

def depend(script_schedule, handler):
    """返回测试数据列表"""
    return [
        {"id": 1, "name": "正常元素1"},
        {"id": 2, "name": "错误元素2"},  # 这个元素会导致错误
        {"id": 3, "name": "正常元素3"},
        {"id": 4, "name": "错误元素4"},  # 这个元素会导致错误
        {"id": 5, "name": "正常元素5"}
    ]

def iteration(script_schedule, handler, depend_item):
    """模拟执行，在特定元素上抛出错误"""
    if depend_item["name"].startswith("错误"):
        raise ValueError(f"测试错误：{depend_item['name']}")
    return {"result": f"处理成功：{depend_item['name']}"}
    '''
    
    # 写入临时测试脚本
    test_script_path = '/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager/scripts/test_error_handling.py'
    with open(test_script_path, 'w', encoding='utf-8') as f:
        f.write(test_script_content)
    
    try:
        handler = ScriptHandler()
        
        # 测试1: is_err_stop=True (默认值)
        print("\n测试1: is_err_stop=True (遇到错误立即终止)")
        print("=" * 60)
        result1 = handler.iterate_script('test_error_handling', is_err_stop=True)
        
        print(f"执行结果: {'成功' if result1['success'] else '失败'}")
        print(f"总迭代次数: {result1['total_iterations']}")
        print(f"成功迭代次数: {result1['successful_iterations']}")
        print(f"失败迭代次数: {result1['failed_iterations']}")
        print(f"错误元素数量: {len(result1['error_items'])}")
        print(f"是否在第一个错误处停止: {result1['failed_iterations'] == 1 and len(result1['error_items']) == 1}")
        
        # 测试2: is_err_stop=False
        print("\n测试2: is_err_stop=False (遇到错误继续执行)")
        print("=" * 60)
        result2 = handler.iterate_script('test_error_handling', is_err_stop=False)
        
        print(f"执行结果: {'成功' if result2['success'] else '失败'}")
        print(f"总迭代次数: {result2['total_iterations']}")
        print(f"成功迭代次数: {result2['successful_iterations']}")
        print(f"失败迭代次数: {result2['failed_iterations']}")
        print(f"错误元素数量: {len(result2['error_items'])}")
        print(f"是否记录了所有错误元素: {result2['failed_iterations'] == 2 and len(result2['error_items']) == 2}")
        
        # 打印错误元素详情
        if result2['error_items']:
            print("\n错误元素详情:")
            for item in result2['error_items']:
                print(f"  索引: {item['index']}, 元素: {item['depend_item']}, 错误: {item['error']}")
        
        # 验证测试结果
        test1_passed = result1['failed_iterations'] == 1 and len(result1['error_items']) == 1
        test2_passed = result2['failed_iterations'] == 2 and len(result2['error_items']) == 2
        
        print("\n" + "=" * 60)
        if test1_passed and test2_passed:
            print("✅ 所有测试通过！")
            return True
        else:
            print("❌ 测试失败！")
            return False
            
    finally:
        # 清理临时测试脚本
        if os.path.exists(test_script_path):
            os.remove(test_script_path)
            print(f"\n清理临时测试脚本: {test_script_path}")

if __name__ == "__main__":
    success = test_iterate_script_error_handling()
    sys.exit(0 if success else 1)