from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class ScriptSchedule(SQLModel, table=True):
    """
    脚本调度模型
    用于存储脚本的调度配置和同步状态信息
    """
    
    # 主键ID，自增
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 脚本名称
    name: str = Field(index=True, description="脚本名称")
    
    # 执行周期（cron表达式或周期描述）
    period: str = Field(default="", description="执行周期")
    
    # 是否启用调度
    turn_on: bool = Field(default=False, description="是否启用调度")
    
    # 最后一次同步的日期时间（时间参数，而非执行时间，如果为空则表示未同步过）
    last_sync_datetime: Optional[datetime] = Field(default=None, description="最后一次同步日期")
    
    # 下一次同步的日期时间
    next_sync_datetime: Optional[datetime] = Field(default=None, description="下一次同步日期")
    
    # 创建时间
    created_at: Optional[datetime] = Field(default_factory=datetime.now, description="创建时间")
    
    # 更新时间
    updated_at: Optional[datetime] = Field(default_factory=datetime.now, description="更新时间")
    
    # 备注信息
    remark: Optional[str] = Field(default="", description="备注信息")
    
    class Config:
        """Pydantic配置"""
        json_schema_extra = {
            "example": {
                "name": "示例脚本",
                "period": "0 */6 * * *", 
                "catch_up": "now",
                "turn_on": True,
                "last_sync_datetime": None,
                "next_sync_datetime": None,
                "remark": "这是一个示例脚本"
            }
        }