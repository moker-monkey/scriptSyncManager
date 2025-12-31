import re
import json
import sys
import os
from typing import List, Dict, Any


def markdown_to_content_dict(markdown_content, output_file=None):
    """
    将markdown文本转换为以title为key、content为value的字典
    整合了origin2Tree.py、tree2FlatJson.py和flatJson2dictJson.py的功能
    
    参数：
    markdown_content: 输入的markdown文本
    output_file: 输出文件路径（可选）
    
    返回：
    以title为key、content为value的字典
    """
    # 1. 第一步：将markdown转换为树状结构（origin2Tree.py的功能）
    CODE_BLOCK_PATTERN = re.compile(r'```([\s\S]*?)```')
    HEADING_PATTERN = re.compile(r'^(#{1,6})\s+(.*)$')
    
    def _markdown_to_tree(markdown_text):
        result = []
        stack = []
        lines = markdown_text.strip().split('\n')
        code_blocks = CODE_BLOCK_PATTERN.findall(markdown_text)

        for i, line in enumerate(lines):
            # 检查是否在代码块中
            is_in_code_block = False
            for block in code_blocks:
                if line in block:
                    is_in_code_block = True
                    break
            if is_in_code_block:
                continue

            match = HEADING_PATTERN.match(line)
            if match:
                level = len(match.group(1))
                title = match.group(2)
                new_item = {
                    'title': title,
                    'content': '',
                    'children': []
                }

                if not stack:
                    result.append(new_item)
                    stack.append((level, new_item))
                else:
                    while stack and stack[-1][0] >= level:
                        stack.pop()
                    if stack:
                        parent = stack[-1][1]
                        parent['children'].append(new_item)
                    else:
                        result.append(new_item)
                    stack.append((level, new_item))

                # 收集内容
                content_lines = []
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    next_match = HEADING_PATTERN.match(next_line)
                    if next_match:
                        break
                    content_lines.append(next_line)
                    j += 1
                new_item['content'] = '\n'.join(content_lines).strip()

        return result
    
    # 2. 第二步：将树状结构转换为扁平化JSON（tree2FlatJson.py的功能）
    def _tree_to_flat_json(tree_data):
        def _process_node(node, parent_titles=None):
            if parent_titles is None:
                parent_titles = []

            json_list = []
            current_titles = parent_titles + [node["title"]]

            if node.get('content', '').strip():
                if parent_titles:
                    title_part = '-'.join(current_titles)
                else:
                    title_part = node["title"]

                json_data = {
                    "title": title_part,
                    "content": f'### {title_part}\n\n{node["content"]}'
                }
                json_list.append(json_data)

            for child in node.get('children', []):
                json_list.extend(_process_node(child, current_titles))

            return json_list
        
        all_json_data = []
        for item in tree_data:
            json_data = _process_node(item)
            all_json_data.extend(json_data)
        return all_json_data
    
    # 3. 第三步：将扁平化JSON转换为字典（flatJson2dictJson.py的功能）
    def _flat_json_to_dict(flat_data: List[Dict[str, Any]]) -> Dict[str, str]:
        result_dict = {}
        processed_count = 0
        interface_count = 0
        
        # 检查输入类型
        if not isinstance(flat_data, list):
            raise TypeError(f"期望输入为列表类型，但得到: {type(flat_data).__name__}")
        
        # 遍历列表中的每个字典
        for index, item in enumerate(flat_data):
            processed_count += 1
            try:
                # 检查必要字段是否存在
                if 'title' not in item:
                    raise KeyError(f"第{index}个字典缺少'title'字段")
                if 'content' not in item:
                    raise KeyError(f"第{index}个字典缺少'content'字段")
                
                # 只保留内容中包含"接口:"的条目
                if '接口:' in item['content']:
                    interface_count += 1
                    # 添加到结果字典
                    if item['title'] in result_dict:
                        print(f"第{index}个字典的'title'字段与已有键重复: {item['title']}")
                    result_dict[item['title']] = item['content']
                
            except Exception as e:
                # 提供更详细的错误信息
                raise type(e)(f"处理第{index}个字典时出错: {str(e)}")
        
        return result_dict, interface_count, processed_count
    
    # 执行完整流程
    print("正在将markdown转换为树状结构...")
    tree_data = _markdown_to_tree(markdown_content)
    
    print("正在将树状结构转换为扁平化JSON...")
    flat_data = _tree_to_flat_json(tree_data)
    
    print(f"正在将扁平化JSON转换为字典，共{len(flat_data)}个条目...")
    result_dict, interface_count, processed_count = _flat_json_to_dict(flat_data)
    
    # 输出结果
    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {output_file}")
    
    print(f"转换完成! 总共处理了 {processed_count} 个条目")
    print(f"提取了 {interface_count} 个包含'接口:'的条目")
    print(f"生成了{len(result_dict)}个键值对")
    return result_dict


def markdown_file_to_content_dict(markdown_file_path, output_file=None):
    """
    从markdown文件转换为以title为key、content为value的字典
    
    参数：
    markdown_file_path: markdown文件路径
    output_file: 输出文件路径（可选）
    
    返回：
    以title为key、content为value的字典
    """
    try:
        with open(markdown_file_path, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        return markdown_to_content_dict(markdown_content, output_file)
        
    except FileNotFoundError:
        print(f"错误: 文件 '{markdown_file_path}' 不存在")
        raise
    except Exception as e:
        print(f"处理文件时发生错误: {str(e)}")
        raise


# 示例用法
if __name__ == '__main__':
    if len(sys.argv) == 2:
        # 仅输入文件，不指定输出
        markdown_file_to_content_dict(sys.argv[1])
    elif len(sys.argv) == 3:
        # 输入文件和输出文件都指定
        markdown_file_to_content_dict(sys.argv[1], sys.argv[2])
    else:
        output_file = "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/menu/stock.json"
        input_file = "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/markdown/stock.md"
        markdown_file_to_content_dict(input_file, output_file)