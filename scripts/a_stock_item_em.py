import akshare as ak

def init(script,handler,depend):
    # 初始化都是直接覆盖表
    return ak.stock_info_a_code_name()

def depend_run(script,handler,depend):
    # 按依赖运行,
    return None



def print_a_stock_item_em(stack_code):
    # 查看股票详情
    print(ak.stock_individual_info_em(symbol=stack_code).set_index('item')['value'].to_frame().T)
    

