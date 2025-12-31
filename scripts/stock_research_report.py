import akshare as ak

def init(script,handler,depend):
    stock_profit_forecast_em_df = ak.stock_profit_forecast_em()
    print(stock_profit_forecast_em_df)
    
    
def print_board_industry(script,handler):
    print(ak.stock_board_industry_name_em())