#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试脚本：验证convert_menu功能是否正确保留last_sync_datetime字段
"""

import os
import sys
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlmodel import SQLModel

# 设置使用SQLite数据库
os.environ["USE_SQLITE"] = "true"

# 添加项目根目录到Python路径
sys.path.append('/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager')

from core.config import config
from core.models import ScriptSchedule

def test_last_sync_preservation():
    """
    测试convert_menu功能是否正确保留last_sync_datetime字段
    """
    # 获取数据库连接
    db_uri = config.get_db_uri()
    engine = create_engine(db_uri)
    
    with engine.connect() as conn:
        # 1. 查询一个脚本
        query = text("SELECT * FROM ScriptSchedule LIMIT 1")
        result = conn.execute(query)
        row = result.fetchone()
        
        if not row:
            print("数据库中没有找到脚本记录，无法进行测试")
            return False
        
        script_id = row.id
        original_last_sync = row.last_sync_datetime
        script_name = row.name
        
        print(f"找到脚本: {script_name}, ID: {script_id}")
        print(f"原始last_sync_datetime: {original_last_sync}")
        
        # 2. 手动设置last_sync_datetime
        new_last_sync = datetime.now() - timedelta(days=5)  # 5天前
        update_query = text(f"UPDATE ScriptSchedule SET last_sync_datetime = :new_value WHERE id = :id")
        conn.execute(update_query, {"new_value": new_last_sync, "id": script_id})
        conn.commit()
        
        print(f"手动设置last_sync_datetime为: {new_last_sync}")
        
        # 3. 重新查询以确认设置成功
        query = text("SELECT * FROM ScriptSchedule WHERE id = :id")
        result = conn.execute(query, {"id": script_id})
        row = result.fetchone()
        print(f"设置后last_sync_datetime: {row.last_sync_datetime}")
        
        # 4. 运行convert_menu
        print("\n运行convert_menu...")
        from core.handler import ScriptHandler
        handler = ScriptHandler()
        result = handler.convert_menu()
        
        # 5. 检查convert_menu后的last_sync_datetime
        query = text("SELECT * FROM ScriptSchedule WHERE id = :id")
        result = conn.execute(query, {"id": script_id})
        row = result.fetchone()
        updated_last_sync = row.last_sync_datetime
        
        print(f"\n转换后last_sync_datetime: {updated_last_sync}")
        
        # 6. 验证是否保留
        # 需要处理日期时间值可能是字符串的情况
        if isinstance(updated_last_sync, str):
            # 如果是字符串，转换为日期时间对象进行比较
            try:
                updated_last_sync = datetime.fromisoformat(updated_last_sync)
            except ValueError:
                # 如果转换失败，可能是数据库返回的格式，尝试使用strptime
                try:
                    updated_last_sync = datetime.strptime(updated_last_sync, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"❌ 测试失败：无法解析last_sync_datetime格式: {updated_last_sync}")
                    return False
        
        # 现在比较日期部分，因为时间可能精度不同
        original_date_str = new_last_sync.strftime("%Y-%m-%d")
        updated_date_str = updated_last_sync.strftime("%Y-%m-%d")
        
        if original_date_str == updated_date_str:
            print("✅ 测试通过：last_sync_datetime被正确保留")
            return True
        else:
            print(f"❌ 测试失败：last_sync_datetime被修改")
            print(f"   预期: {original_date_str}")
            print(f"   实际: {updated_date_str}")
            return False

def test_last_sync_preservation_new_record():
    """
    测试当新创建记录时，last_sync_datetime是否正确处理
    """
    # 获取数据库连接
    db_uri = config.get_db_uri()
    engine = create_engine(db_uri)
    
    with engine.connect() as conn:
        # 1. 检查是否存在某个特定脚本
        test_script_name = "test_script_for_last_sync"
        
        query = text("SELECT * FROM ScriptSchedule WHERE name = :name")
        result = conn.execute(query, {"name": test_script_name})
        row = result.fetchone()
        
        # 如果存在，则删除
        if row:
            delete_query = text("DELETE FROM ScriptSchedule WHERE name = :name")
            conn.execute(delete_query, {"name": test_script_name})
            conn.commit()
            print(f"删除了已存在的测试脚本: {test_script_name}")
        
        # 2. 添加一个测试条目到Menu.json中
        print(f"在Menu.json中添加测试脚本: {test_script_name}")
        
        # 读取现有Menu.json
        menu_path = os.path.join(config.base_dir, 'Menu.json')
        with open(menu_path, 'r', encoding='utf-8') as f:
            menu_data = json.load(f)
        
        # 添加测试条目
        test_item = {
            "name": test_script_name,
            "cn_name": "测试脚本",
            "desc": "用于测试last_sync_datetime保留的测试脚本",
            "period": "every_day",
            "turn_on": True
        }
        menu_data.append(test_item)
        
        # 写回Menu.json
        with open(menu_path, 'w', encoding='utf-8') as f:
            json.dump(menu_data, f, ensure_ascii=False, indent=2)
        
        # 3. 运行convert_menu
        print("\n运行convert-menu命令...")
        os.system("USE_SQLITE=true python /Users/xiaochangming/Desktop/agent-trade/scriptSyncManager/manager.py convert-menu > /dev/null 2>&1")
        
        # 4. 查询新创建的脚本记录
        query = text("SELECT * FROM ScriptSchedule WHERE name = :name")
        result = conn.execute(query, {"name": test_script_name})
        row = result.fetchone()
        
        if not row:
            print("❌ 测试失败：新脚本记录未创建")
            return False
        
        print(f"新脚本记录创建成功")
        print(f"脚本名称: {row.name}")
        print(f"创建时间: {row.created_at}")
        print(f"最后同步时间: {row.last_sync_datetime}")
        
        # 5. 验证新记录的last_sync_datetime为空
        if row.last_sync_datetime is None:
            print("✅ 测试通过：新创建记录的last_sync_datetime正确为空")
            return True
        else:
            print(f"❌ 测试失败：新创建记录的last_sync_datetime应为None，但实际为: {row.last_sync_datetime}")
            return False

if __name__ == "__main__":
    print("开始测试last_sync_datetime保留功能...")
    print("=" * 50)
    
    # 测试1: 验证更新现有记录时last_sync_datetime是否被保留
    print("测试1: 验证更新现有记录时last_sync_datetime是否被保留")
    success1 = test_last_sync_preservation()
    
    print("\n" + "=" * 50)
    
    # 测试2: 验证新创建记录时last_sync_datetime是否为空
    print("测试2: 验证新创建记录时last_sync_datetime是否为空")
    success2 = test_last_sync_preservation_new_record()
    
    print("\n" + "=" * 50)
    if success1 and success2:
        print("所有测试通过!")
        sys.exit(0)
    else:
        print("测试失败!")
        sys.exit(1)