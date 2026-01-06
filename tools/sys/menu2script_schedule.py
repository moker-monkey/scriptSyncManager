# 写一个函数,用于将data_menu.json转换为script_schedule.json
# 该函数的逻辑为:
#   1. 递归遍历data_menu.json,如果有type则参考script_schedule.json进行转换
#   2. 不同类型的type有不同类型的默认值
#       1. period为周期数据(周期,初始化时间范围,上一次同步的日期(为空),turn_on统一为true)
#       2. realtime实时数据(周期,初始化时间范围,上一次同步的日期(为空),turn_on统一为true)
#       3. meta数据(周期,上一次同步日期(为空),turn_on统一为true)
#       4. news数据(没有周期,初始化时间范围,只有上一次同步时间(为空)

import json
import os

import pandas as pd


def convert_menu_to_script_schedule(
    menu_path: str,
):
    """ 
    将Menu.json转换为script_schedule_list

    参数:
        menu_path: str, Menu.json文件路径

    返回:
        pd.DataFrame: 转换后的script_schedule DataFrame
    """
    print("menu_path", menu_path)
    try:
        # 读取Menu.json文件
        with open(menu_path, "r", encoding="utf-8") as f:
            menu = json.load(f)

        script_schedule = []

        # 递归遍历函数
        def traverse_items(items):
            for item in items:
                # 是否有turn_on和name字段
                if "name" in item:
                    # 创建script_schedule条目
                    schedule_item = {
                        "name": item["name"],
                        "cn_name": item.get("cn_name", ""),
                        "desc": item.get("desc", ""),
                        "interval": item.get("interval", ""),
                        "is_error_stop": item.get("is_error_stop", False),
                        "type": item.get("type", ""),
                        "schedule": item.get("schedule", {}),
                        "meta": item.get("meta", {})
                    }

                    script_schedule.append(schedule_item)

                # 检查是否有子列表需要遍历
                if "list" in item:
                    traverse_items(item["list"])

        # 开始遍历
        traverse_items(menu)
        
        print(f"转换成功！生成了 {len(script_schedule)} 个调度条目")
        return pd.DataFrame(script_schedule)

    except Exception as e:
        print(f"转换失败：{str(e)}")
        return False


# 默认路径设置

# 测试示例
if __name__ == "__main__":
    DEFAULT_MENU_PATH = os.path.join(os.path.dirname(__file__), "Menu.json")
    # 使用默认路径进行转换
    convert_menu_to_script_schedule(
        DEFAULT_MENU_PATH
    )
