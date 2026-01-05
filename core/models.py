from sqlmodel import SQLModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy import JSON


class ScriptSyncMenu(SQLModel, table=True):
    """
    脚本调度模型
    用于存储脚本的调度配置和同步状态信息
    """
    
    # 主键ID，自增
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 脚本名称
    name: str = Field(index=True, description="脚本名称")
    cn_name: Optional[str] = Field(default=None, description="脚本中文名称")
    desc: Optional[str] = Field(default=None, description="脚本描述")

    meta: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSON, description="脚本元数据")
    
    remark: Optional[str] = Field(default=None, description="备注")
    
    updated_at: Optional[datetime] = Field(default_factory=datetime.now, description="更新时间")
    
    class Config:
        """Pydantic配置"""
        json_schema_extra = {
            "example": {
                "name": "示例脚本",
                "cn_name": "示例脚本",
                "desc": "这是一个示例脚本",
                "catch_up": "now",
                "remark": "这是一个示例脚本"
            }
        }

class ScriptSyncSchedule(SQLModel, table=True):
    """
    是脚本的调度配置表，关联ScriptMenu表，用于存储脚本的调度配置信息
    """
    # 主键ID，自增
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, description="脚本名称")
    turn_on: bool = Field(default=False, description="是否启用调度")
    period: str = Field(default="", description="执行周期")
    last_sync_datetime: Optional[datetime] = Field(default=None, description="最后一次同步日期")
    next_sync_datetime: Optional[datetime] = Field(default=None, description="下一次同步日期")

    class Config:
        """Pydantic配置"""
        json_schema_extra = {
            "example": {
                "name": "示例脚本",
                "turn_on": True,
                "period": "0 */6 * * *",
                "last_sync_datetime": "2023-10-01T12:00:00",
                "next_sync_datetime": "2023-10-01T18:00:00"
            }
        }

class ScriptSyncScheduleQueue(SQLModel, table=True):
    """
    是脚本的调度队列，用于存储待执行的脚本任务
    """
    
    # 主键ID，自增
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 脚本名称
    name: str = Field(index=True, description="脚本名称")
    
    # 执行时间
    execute_time: datetime = Field(description="执行时间")
    
    # 执行状态（成功/失败）
    status: str = Field(description="执行状态")
    
    # 执行结果（JSON格式）
    result: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSON, description="执行结果")
    
    # 创建时间
    created_at: Optional[datetime] = Field(default_factory=datetime.now, description="创建时间")
    
    class Config:
        """Pydantic配置"""
        json_schema_extra = {
            "example": {
                "name": "示例脚本",
                "execute_time": "2023-10-01T12:00:00",
                "status": "success",
                "result": {"key": "value"}
            }
        }