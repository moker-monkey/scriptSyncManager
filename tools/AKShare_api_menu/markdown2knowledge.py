import re
import json
import sys


def markdown_to_menu(markdown_content, output_file=None):
    """
    将markdown文本转换为菜单格式
    整合了origin2Tree.py、tree2FlatJson.py和flatJson2Menu.py的功能
    
    参数：
    markdown_content: 输入的markdown文本
    output_file: 输出文件路径（可选）
    
    返回：
    菜单格式的文本内容
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
    
    # 3. 第三步：从扁平化JSON生成菜单（flatJson2Menu.py的功能）
    def _flat_json_to_menu(flat_data):
        toc_items = []
        interface_count = 0
        processed_count = 0
        
        for item in flat_data:
            processed_count += 1
            if 'content' in item and '接口:' in item['content']:
                interface_count += 1
                title = item.get('title', f'接口 {interface_count}')
                toc_items.append(f"- {title}")
        
        menu_content = '\n'.join(toc_items)
        return menu_content, interface_count, processed_count
    
    # 执行完整流程
    tree_data = _markdown_to_tree(markdown_content)
    flat_data = _tree_to_flat_json(tree_data)
    menu_content, interface_count, processed_count = _flat_json_to_menu(flat_data)
    
    # 输出结果
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(menu_content)
        print(f"处理完成!")
        print(f"总共处理了 {processed_count} 个条目")
        print(f"提取了 {interface_count} 个包含'接口:'的条目")
        print(f"结果已保存到: {output_file}")
    
    return menu_content


def markdown_file_to_menu(markdown_file_path, output_file=None):
    """
    从markdown文件转换为菜单格式
    
    参数：
    markdown_file_path: markdown文件路径
    output_file: 输出文件路径（可选）
    
    返回：
    菜单格式的文本内容
    """
    with open(markdown_file_path, 'r', encoding='utf-8') as f:
        markdown_content = f.read()
    
    return markdown_to_menu(markdown_content, output_file)


# 示例用法
if __name__ == '__main__':
    if len(sys.argv) == 2:
        # 仅输入文件，不指定输出
        markdown_file_to_menu(sys.argv[1])
    elif len(sys.argv) == 3:
        # 输入文件和输出文件都指定
        markdown_file_to_menu(sys.argv[1], sys.argv[2])
    else:
        output_file = "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/menu/stock.md"
        input_file = "/Users/xiaochangming/Desktop/agent-trade/akshare/data/meta/markdown/stock.md"
        markdown_file_to_menu(input_file, output_file)