# 实现基于crontab的下次执行时间计算功能

## 1. 需求分析
- 当前代码在执行脚本后直接将`last_sync_datetime`设置为当前时间
- 需要根据crontab表达式和最后执行时间计算`next_sync_datetime`
- crontab表达式存储在`ScriptSchedule`模型的`period`字段中

## 2. 实现方案

### 2.1 安装依赖库
- 安装`croniter`库用于解析crontab表达式和计算下次执行时间

### 2.2 添加计算下次执行时间的函数
在`ScriptHandler`类中添加一个方法：
```python
def _calculate_next_sync_time(self, last_sync_time: datetime, cron_expression: str) -> Optional[datetime]:
    """
    根据上次执行时间和crontab表达式计算下次执行时间
    
    Args:
        last_sync_time (datetime): 上次执行时间
        cron_expression (str): crontab表达式
        
    Returns:
        Optional[datetime]: 下次执行时间，计算失败返回None
    """
    pass
```

### 2.3 更新同步时间逻辑
在handler.py第434行附近，更新代码逻辑：
1. 设置`last_sync_datetime`为当前时间
2. 调用新函数计算`next_sync_datetime`
3. 将`next_sync_datetime`更新到数据库

## 3. 代码修改点
- `/Users/xiaochangming/Desktop/agent-trade/scriptSyncManager/core/handler.py`：添加计算函数并更新同步逻辑

## 4. 测试计划
- 验证不同crontab表达式的计算结果是否正确
- 验证更新到数据库的时间是否准确