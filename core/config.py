import os
from typing import Dict, Any, Optional
from sqlmodel import create_engine, SQLModel
from .models import ScriptSchedule


class Config:
    """
    项目配置类，封装数据库操作与路径定义
    """
    
    def __init__(self):
        """初始化配置，支持环境变量覆盖"""
        # 检查是否使用SQLite数据库进行测试
        self._use_sqlite = os.getenv("USE_SQLITE", "true").lower() == "true"
        
        if self._use_sqlite:
            # SQLite配置
            self._sqlite_db_path = os.getenv("SQLITE_DB_PATH", os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "script_sync_manager.db"
            ))
            
            # 创建SQLite数据库目录
            os.makedirs(os.path.dirname(self._sqlite_db_path), exist_ok=True)
        else:
            # MySQL配置
            self._db_host = os.getenv("DB_HOST", "localhost")
            self._db_port = int(os.getenv("DB_PORT", "3306"))
            self._db_user = os.getenv("DB_USER", "root")
            self._db_password = os.getenv("DB_PASSWORD", "")
            self._db_name = os.getenv("DB_NAME", "project_db")
        
        # 项目路径定义
        self._base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._log_dir = f"{self._base_dir}/logs"
        self._data_dir = f"{self._base_dir}/data"

    @property
    def db_host(self) -> str:
        """数据库主机地址"""
        return self._db_host

    @property
    def db_port(self) -> int:
        """数据库端口"""
        return self._db_port

    @property
    def db_user(self) -> str:
        """数据库用户名"""
        return self._db_user

    @property
    def db_password(self) -> str:
        """数据库密码"""
        return self._db_password

    @property
    def db_name(self) -> str:
        """数据库名称"""
        return self._db_name

    @property
    def base_dir(self) -> str:
        """项目根目录"""
        return self._base_dir

    @property
    def log_dir(self) -> str:
        """日志目录"""
        return self._log_dir

    @property
    def data_dir(self) -> str:
        """数据目录"""
        return self._data_dir

    def validate_config(self) -> bool:
        """
        验证配置的有效性
        
        Returns:
            bool: 配置是否有效
        """
        if self._use_sqlite:
            # SQLite模式不需要验证MySQL连接参数
            return True
        
        if not self._db_host:
            raise ValueError("数据库主机地址不能为空")
        
        if not (1 <= self._db_port <= 65535):
            raise ValueError("数据库端口必须在 1-65535 范围内")
        
        if not self._db_user:
            raise ValueError("数据库用户名不能为空")
        
        if not self._db_name:
            raise ValueError("数据库名称不能为空")
        
        return True

    def get_db_uri(self) -> str:
        """
        获取数据库连接字符串
        
        Returns:
            str: 数据库连接 URI
        """
        if self._use_sqlite:
            return f"sqlite:///{self._sqlite_db_path}"
        else:
            return (
                f"mysql+pymysql://{self._db_user}:{self._db_password}"
                f"@{self._db_host}:{self._db_port}/{self._db_name}?charset=utf8mb4"
            )

    def get_script_db_uri(self) -> str:
        """
        当前脚本库和主库使用相同的库，但未来可能会分开存储
        获取脚本数据库连接字符串
        
        Returns:
            str: 脚本数据库连接 URI
        """
        if self._use_sqlite:
            script_db_path = os.path.join(
                os.path.dirname(self._sqlite_db_path),
                "script_sync_manager.db" # 以后可以修改此处使得脚本库和主库可以分开存储
            )
            return f"sqlite:///{script_db_path}"
        else:
            script_db_name = os.getenv("SCRIPT_DB_NAME", "script_db")
            return (
                f"mysql+pymysql://{self._db_user}:{self._db_password}"
                f"@{self._db_host}:{self._db_port}/{script_db_name}?charset=utf8mb4"
            )

    def init_db(self) -> Dict[str, Any]:
        """
        初始化数据库（结合 SQLModel，非 Web 场景）
        
        Returns:
            Dict[str, Any]: 包含数据库引擎的字典
        """
        # 验证配置
        self.validate_config()
        
        # 主库引擎
        engine = create_engine(
            self.get_db_uri(),
            echo=False,
            pool_pre_ping=True,
        )

        # 脚本库引擎
        script_engine = create_engine(
            self.get_script_db_uri(),
            echo=False,
            pool_pre_ping=True,
        )

        # 按需建表（使用具体模型类）
        ScriptSchedule.metadata.create_all(engine)

        return {"engine": engine, "script_engine": script_engine}
    
    def get_script_log_dir(self, script_name: str) -> str:
        """
        根据脚本名称返回对应的日志目录路径
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            str: 日志目录路径
        """
        return f"{self._log_dir}/{script_name}"

    def get_script_data_dir(self, script_name: str) -> str:
        """
        根据脚本名称返回对应的数据目录路径
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            str: 数据目录路径
        """
        return f"{self._data_dir}/{script_name}"
    
    def get_script_breakpoint_dir(self, script_name: str) -> str:
        """
        根据脚本名称返回对应的断点信息目录路径
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            str: 断点信息目录路径
        """
        breakpoint_dir = f"{self._base_dir}/breakpoints"
        return f"{breakpoint_dir}/{script_name}"

    def create_directories(self) -> None:
        """
        创建必要的目录结构
        """
        # 创建根目录
        os.makedirs(self._base_dir, exist_ok=True)
        
        # 创建日志目录
        os.makedirs(self._log_dir, exist_ok=True)
        
        # 创建数据目录
        os.makedirs(self._data_dir, exist_ok=True)


# 创建全局配置实例
config = Config()

    
