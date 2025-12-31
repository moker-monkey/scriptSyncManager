import os
import sys
from typing import List, Dict, Any

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入需要的函数
from markdown2knowledge import markdown_file_to_menu
from markdown2Dict import markdown_file_to_content_dict


def process_markdown_folder(
    input_folder: str, is_save: bool = False, output_folder: str = ""
) -> Dict[str, Any]:
    """
    遍历markdown文件夹并将每个文件分别输入到两个转换函数

    参数：
    input_folder: 输入的markdown文件夹路径
    output_folder: 输出文件夹路径
    is_save: 是否保存到文件，默认为True

    返回：
    如果is_save为False，返回{"data": {"menu": 合并后的菜单内容, "dict": 合并后的字典内容}}
    如果is_save为True，返回{"status": "success"}
    """
    # 如果需要保存到文件，确保输出文件夹存在
    if is_save:
        os.makedirs(output_folder, exist_ok=True)

    # 用于存储处理结果
    results = {"menu": "", "dict": {}}

    # 遍历输入文件夹中的所有文件
    for filename in os.listdir(input_folder):
        if filename.endswith(".md"):
            # 构建输入文件路径
            input_file = os.path.join(input_folder, filename)

            # 获取文件名（不含扩展名）
            file_name_without_ext = os.path.splitext(filename)[0]

            # 构建输出文件路径
            menu_output_file = os.path.join(
                output_folder, f"{file_name_without_ext}.md"
            )
            dict_output_file = os.path.join(
                output_folder, f"{file_name_without_ext}.json"
            )

            print(f"\n处理文件: {filename}")

            # 调用markdown2knowledge.py中的函数生成菜单格式
            print(f"正在生成菜单格式...")
            try:
                if is_save:
                    menu_content = markdown_file_to_menu(input_file, menu_output_file)
                    print(f"菜单格式已保存到: {menu_output_file}")
                else:
                    menu_content = markdown_file_to_menu(input_file)
                    print(f"菜单格式已生成")
            except Exception as e:
                print(f"生成菜单格式时出错: {str(e)}")
                menu_content = None

            # 调用markdown2Dict.py中的函数生成字典格式
            print(f"正在生成字典格式...")
            try:
                if is_save:
                    dict_content = markdown_file_to_content_dict(
                        input_file, dict_output_file
                    )
                    print(f"字典格式已保存到: {dict_output_file}")
                else:
                    dict_content = markdown_file_to_content_dict(input_file)
                    print(f"字典格式已生成")
            except Exception as e:
                print(f"生成字典格式时出错: {str(e)}")
                dict_content = None

            # 如果不保存到文件，收集并合并结果
            if not is_save:
                if menu_content:
                    # 合并菜单内容，每个文件的菜单内容用空行分隔
                    results["menu"] += menu_content + "\n\n"
                if dict_content and isinstance(dict_content, dict):
                    # 合并字典内容，将每个文件的字典合并到总字典中
                    results["dict"].update(dict_content)

            print(f"文件 {filename} 处理完成!")

    print(f"\n所有文件处理完成!")

    # 根据is_save参数返回不同的结果
    if is_save:
        return {"status": "success"}
    else:
        # 去除菜单内容末尾多余的空行
        if results["menu"].endswith("\n\n"):
            results["menu"] = results["menu"][:-2]
        return {"data": results}


# 示例用法
if __name__ == "__main__":
    # 默认的输入输出路径
    DEFAULT_INPUT_FOLDER = (
        "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/markdown"
    )
    DEFAULT_OUTPUT_FOLDER = (
        "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/menu"
    )

    if len(sys.argv) == 3:
        # 用户指定了输入和输出文件夹
        input_folder = sys.argv[1]
        output_folder = sys.argv[2]
        process_markdown_folder(input_folder, output_folder)
    elif len(sys.argv) == 4:
        # 用户指定了输入、输出文件夹和is_save参数
        input_folder = sys.argv[1]
        output_folder = sys.argv[2]
        is_save = sys.argv[3].lower() == "true"
        result = process_markdown_folder(input_folder, output_folder, is_save)
        if not is_save:
            print(f"处理结果: {result}")
    else:
        # 使用默认路径
        print(f"使用默认路径:")
        print(f"输入文件夹: {DEFAULT_INPUT_FOLDER}")
        print(f"输出文件夹: {DEFAULT_OUTPUT_FOLDER}")
        process_markdown_folder(DEFAULT_INPUT_FOLDER, DEFAULT_OUTPUT_FOLDER)
