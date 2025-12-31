import json
import os

import pandas as pd
from tools.AKShare_api_menu.autoTransform import process_markdown_folder


def init(script, handler, depend):
    """
    初始化脚本
    depend:
        {'filename': ['tool.md', 'bound.md', 'currency.md', 'fx.md', 'index.md', 'interest_rate.md', 'features.md', 'stock.md', 'marco.md']}
    """
    # 1. 通过依赖中的filename获取到文件名列表
    # 2. 使用tools.AKShare_api_menu中的get_akshare_api函数获取到api列表
    print("获取到依赖了:", depend)
    data = process_markdown_folder(os.path.join(handler.config.data_dir, "markdown/"))
    menu_path = os.path.join(handler.config.data_dir, "menu.md")
    with open(menu_path, "w", encoding="utf-8") as f:
        f.write(data.get("data", {}).get("menu", ""))
    # 获取合并后的字典内容
    content_dict = data.get("data", {}).get("dict", {})
    df = (
        pd.DataFrame.from_dict(content_dict, orient="index", columns=["content"])
        .reset_index()
        .rename(columns={"index": "title"})
    )
    json_file_path = os.path.join(handler.config.data_dir, "menu_dict.json")
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(content_dict, f, ensure_ascii=False, indent=4)
    print(f"字典已保存到JSON文件: {json_file_path}")
    # 打印DataFrame的基本信息
    print(f"\n转换后的DataFrame信息：")
    print(f"总行数：{len(df)}")
    print(f"列名：{list(df.columns)}")

    return df


def depend(script, handler):
    """
    依赖列表,返回一个字符串列表,每个字符串是一个文件名,一旦存在该函数,就会在脚本执行前执行,并且可以将依赖当做init和period的参数传入
    """
    origin_markdown = os.path.join(handler.config.data_dir, "markdown/")

    # 获取文件夹下的文件名列表
    try:
        # 检查路径是否存在
        if os.path.exists(origin_markdown):
            # 获取所有文件和文件夹
            items = os.listdir(origin_markdown)
            # 过滤出只有文件的列表
            file_names = [
                item
                for item in items
                if os.path.isfile(os.path.join(origin_markdown, item))
            ]
            return {"filename": file_names}
        else:
            print(f"警告: 路径不存在 - {origin_markdown}")
            return {"filename": []}
    except Exception as e:
        print(f"获取文件名列表时发生错误: {e}")
        return {"filename": []}


def create_menu_dict(script, handler):
    """
    会首先从表中读数据,然后根据title和content创建一个字典,之后存储到对应data的AKShare_api_menu中
    """
    print("从表中读取数据...")
    df = handler.config.query_script_table("AKShare_api_menu", "SELECT *")
    menu_dict = {}
    for _, row in df.iterrows():
        title = row["title"]
        content = row["content"]
        menu_dict[title] = content
    data_dir = os.path.join(handler.config.data_dir, "AKShare_api_menu/")
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, "menu_dict.json"), "w", encoding="utf-8") as f:
        json.dump(menu_dict, f, ensure_ascii=False, indent=4)
    print(f"创建菜单字典完成，共{len(menu_dict)}个项")
