# 从指定的数据库表中读取数据,并将其转换为JSON格式
import pandas as pd
from sqlalchemy import text
from core.config import Config

def read_table_to_json(table_name, query=""):
    """
    从指定的数据库表中读取数据,并将其转换为JSON格式
    """
    # 从配置中获取数据库引擎
    config = Config()
    engine = config.script_db_engine
    # 构建SQL查询
    if query:
        sql_query = text(query)
    else:
        sql_query = text(f"SELECT * FROM {table_name}")
    # 从数据库中读取数据
    df = pd.read_sql(sql_query, engine)
    # 将数据转换为JSON格式
    json_data = df.to_json(orient="records", force_ascii=False)
    return json_data

