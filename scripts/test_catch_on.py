#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本：验证 catch_on 断点续跑功能
"""

from datetime import datetime


def depend(script_schedule, self):
    """
    返回需要处理的依赖数据
    """
    # 返回一个包含多个元素的列表，每个元素都是字典
    return [
        {"id": 1, "name": "数据1", "should_fail": False},
        {"id": 2, "name": "数据2", "should_fail": False},
        {"id": 3, "name": "数据3", "should_fail": True},  # 这个会失败
        {"id": 4, "name": "数据4", "should_fail": False},
        {"id": 5, "name": "数据5", "should_fail": False},
    ]


def iteration(script_schedule, self, depend_item):
    """
    处理每个依赖项
    """
    print(f"处理数据: {depend_item}")
    
    # 模拟处理逻辑
    if depend_item.get("should_fail", False):
        # 模拟失败的情况
        print(f"  -> 数据 {depend_item['id']} 处理失败")
        raise Exception(f"模拟失败 - 数据 {depend_item['id']}")
    else:
        # 模拟成功的情况
        print(f"  -> 数据 {depend_item['id']} 处理成功")
        return {
            "processed_at": datetime.now().isoformat(),
            "data_id": depend_item["id"],
            "data_name": depend_item["name"],
            "status": "success"
        }


def test_manual_catch_on():
    """
    手动测试 catch_on 功能
    """
    from core.handler import ScriptHandler
    
    handler = ScriptHandler()
    script_name = "test_catch_on"
    
    print("=" * 60)
    print("测试 catch_on 断点续跑功能")
    print("=" * 60)
    
    # 1. 先执行 iterate_script，模拟中途失败的情况
    print("\n1. 执行 iterate_script，模拟中途失败")
    result1 = handler.iterate_script(script_name)
    print(f"结果: {result1['message']}")
    print(f"成功次数: {result1['successful_iterations']}")
    print(f"失败次数: {result1['failed_iterations']}")
    
    # 2. 检查断点信息是否已保存
    print("\n2. 检查断点信息")
    breakpoint_info = handler._load_latest_breakpoint_info(script_name)
    if breakpoint_info:
        print(f"断点信息已保存:")
        print(f"  - 总项数: {breakpoint_info['total_items']}")
        print(f"  - 成功项数: {breakpoint_info['successful_items']}")
        print(f"  - 失败项数: {breakpoint_info['failed_items']}")
        print(f"  - 最后成功索引: {breakpoint_info['last_successful_index']}")
    else:
        print("没有找到断点信息")
        return
    
    # 3. 执行 catch_on 进行断点续跑
    print("\n3. 执行 catch_on 断点续跑")
    result2 = handler.catch_on(script_name)
    print(f"结果: {result2['message']}")
    print(f"继续执行次数: {result2['total_continued']}")
    print(f"成功次数: {result2['successful_continued']}")
    print(f"失败次数: {result2['failed_continued']}")
    
    # 4. 检查最终状态
    print("\n4. 检查最终状态")
    final_breakpoint = handler._load_latest_breakpoint_info(script_name)
    if final_breakpoint:
        print(f"最终断点信息:")
        print(f"  - 总项数: {final_breakpoint['total_items']}")
        print(f"  - 成功项数: {final_breakpoint['successful_items']}")
        print(f"  - 失败项数: {final_breakpoint['failed_items']}")
        print(f"  - 最后成功索引: {final_breakpoint['last_successful_index']}")
    else:
        print("断点信息已清除，说明所有任务都已完成")
    
    print("\n" + "=" * 60)
    print("测试完成")


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    test_manual_catch_on()