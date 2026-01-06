import os,sys
from datetime import datetime
from typing import Dict, Any, Optional, List
from sqlmodel import create_engine, Session, select
from sqlmodel import text
from .models import ScriptSyncMenu,ScriptSyncSchedule


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


    def get_table_data(self, script_name: str,condtion:Dict[str,Any] = None,limit:int = None) -> Optional[List[Dict[str, Any]]]:
        """
        根据脚本名称获取对应的数据表
        
        Args:
            script_name (str): 脚本名称
            condtion (Dict[str,Any], optional): 查询条件，默认 None
            limit (int, optional): 限制返回数据条数，默认 None（返回全部数据）
            
        Returns:
            Optional[List[Dict[str, Any]]]: 包含表数据的列表，若不存在则返回 None
        """
        # 定义脚本与数据表的映射关系
        # 直接以 script_name 作为表名，无需映射
        print('get_table_data start')

        table_name = script_name
        
        if not table_name:
            return None
            
        try:
            # 构建查询条件
            if condtion:
                condtion_str = " AND ".join([f"{k} = :{k}" for k in condtion.keys()])
                where_clause = f" WHERE {condtion_str}"
            else:
                where_clause = ""
                condtion = {}

            # 构建LIMIT子句
            limit_clause = f" LIMIT {limit}" if limit is not None else ""

            # 从数据库查询数据
            engine = self.init_db()["engine"]
            with Session(engine) as session:
                # 直接以原生 SQL 查询，无需模型定义
                result = session.execute(
                    text(f"SELECT * FROM {table_name}{where_clause}{limit_clause}"),
                    condtion
                )
            # result 是 Row 对象列表，需转成 dict 列表
            # 在 SQLAlchemy 2.0 中，Row 对象需要使用 _asdict() 方法转换为字典
            return [row._asdict() for row in result] if result else None
        except Exception as e:
            # 处理数据库异常，如表不存在、锁定等情况
            print(f"获取表 {table_name} 数据时发生错误: {e}")
            return None

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
        ScriptSyncMenu.metadata.create_all(engine)
        ScriptSyncSchedule.metadata.create_all(engine)

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

    def convert_menu(self, menu_path: str = None) -> Dict[str, Any]:
        """
        将 Menu.json 转换为脚本调度配置并更新数据库

        Args:
            menu_path (str): Menu.json 文件路径，如果为 None 则使用项目根目录的 Menu.json

        Returns:
            Dict[str, Any]: 转换和更新结果
        """
        result = {
            "success": False,
            "menu_path": "",
            "total_items": 0,
            "created_items": 0,
            "updated_items": 0,
            "skipped_items": 0,
            "message": "",
            "details": [],
        }

        try:
            # 1. 确定 Menu.json 路径
            if menu_path is None:
                menu_path = os.path.join(config.base_dir, "Menu.json")
            result["menu_path"] = menu_path

            print(f"开始转换 Menu.json: {menu_path}")

            # 2. 导入工具模块并调用转换函数
            tools_dir = os.path.join(config.base_dir, "tools", "sys")
            if not os.path.exists(tools_dir):
                raise FileNotFoundError(f"工具目录不存在: {tools_dir}")

            # 添加工具目录到 Python 路径
            if tools_dir not in sys.path:
                sys.path.insert(0, tools_dir)

            try:
                from menu2script_schedule import convert_menu_to_script_schedule

                print("成功导入转换工具函数")
            except ImportError as e:
                raise ImportError(f"无法导入 menu2script_schedule 工具: {str(e)}")

            # 3. 执行转换
            schedule_df = convert_menu_to_script_schedule(menu_path)

            if schedule_df is False or schedule_df.empty:
                raise ValueError("转换失败或返回空数据")

            result["total_items"] = len(schedule_df)
            print(f"转换完成，共生成 {result['total_items']} 个调度条目")

            # 4. 获取数据库连接
            engines = config.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                for _, row in schedule_df.iterrows():
                    script_name = row["name"]
                    print(f"处理脚本: {script_name}")

                    # 检查 ScriptSyncMenu 是否已存在
                    existing_menu = (
                        session.query(ScriptSyncMenu)
                        .filter(ScriptSyncMenu.name == script_name)
                        .first()
                    )

                    # 检查 ScriptSyncSchedule 是否已存在
                    existing_schedule = (
                        session.query(ScriptSyncSchedule)
                        .filter(ScriptSyncSchedule.name == script_name)
                        .first()
                    )

                    schedule = row.get("schedule")

                    if existing_menu and existing_schedule:
                        # 保留原有的 last_sync_datetime
                        original_last_sync = existing_schedule.last_sync_datetime

                        # 更新 ScriptSyncMenu
                        existing_menu.updated_at = datetime.now()
                        session.add(existing_menu)

                        # 更新 ScriptSyncSchedule
                        existing_schedule.period = schedule.get("period", "")
                        existing_schedule.turn_on = schedule.get("turn_on", False)
                        existing_schedule.start_time = schedule.get("start_time", None)
                        existing_schedule.end_time = schedule.get("end_time", None)
                        existing_schedule.step = schedule.get("step", "")
                        existing_schedule.immediate = schedule.get("immediate", False)

                        # 只在原值为空时才设置新的 last_sync_datetime
                        if original_last_sync is None and schedule.get("last_sync_datetime"):
                            existing_schedule.last_sync_datetime = schedule.get("last_sync_datetime")

                        session.add(existing_schedule)
                        result["updated_items"] += 1

                        detail = {
                            "script_name": script_name,
                            "action": "updated",
                            "last_sync_preserved": original_last_sync is not None,
                            "period": existing_schedule.period,
                            "turn_on": existing_schedule.turn_on,
                        }
                        result["details"].append(detail)

                        print(
                            f"更新脚本调度: {script_name} (保留 last_sync_datetime: {original_last_sync is not None})")

                    else:
                        # 创建新记录 - 同时创建 ScriptSyncMenu 和 ScriptSyncSchedule
                        new_menu = ScriptSyncMenu(
                            name=script_name,
                            remark=f"自动创建",
                        )
                        session.add(new_menu)

                        new_schedule = ScriptSyncSchedule(
                            name=script_name,
                            period=schedule.get("period", ""),
                            turn_on=schedule.get("turn_on", False),
                            last_sync_datetime=None,  # 新记录没有最后同步时间
                            start_time=schedule.get("start_time", None),
                            end_time=schedule.get("end_time", None),
                            step=schedule.get("step", ""),
                            immediate=schedule.get("immediate", False),
                        )
                        session.add(new_schedule)
                        result["created_items"] += 1

                        detail = {
                            "script_name": script_name,
                            "action": "created",
                            "period": new_schedule.period,
                            "turn_on": new_schedule.turn_on,
                        }
                        result["details"].append(detail)

                        print(f"创建新脚本调度: {script_name}")

                # 提交事务
                session.commit()

            # 5. 生成结果消息
            message_parts = []
            if result["created_items"] > 0:
                message_parts.append(f"创建了 {result['created_items']} 个新条目")
            if result["updated_items"] > 0:
                message_parts.append(f"更新了 {result['updated_items']} 个条目")

            result["message"] = (
                f"Menu.json 转换成功！共处理 {result['total_items']} 个条目，{', '.join(message_parts)}")
            result["success"] = True

            print(f"Menu.json 转换完成: {result['message']}")

        except Exception as e:
            error_msg = f"Menu.json 转换失败: {str(e)}"
            result["message"] = error_msg
            print(error_msg)

        return result
    
    def get_task_schedule_list(self) -> List[Dict[str, Any]]:
        """
        从数据库获取任务调度列表

        Returns:
            List[Dict[str, Any]]: 任务调度列表
        """
        try:
            # 获取数据库连接
            engines = self.init_db()
            engine = engines["engine"]

            with Session(engine) as session:
                # 查询所有 ScriptSyncSchedule 记录
                schedule_list = session.query(ScriptSyncSchedule).all()
                
                # 转换为字典列表
                result = []
                for schedule in schedule_list:
                    # 将模型转换为字典
                    schedule_dict = {
                        "id": schedule.id,
                        "name": schedule.name,
                        "period": schedule.period,
                        "turn_on": schedule.turn_on,
                        "start_time": schedule.start_time,
                        "end_time": schedule.end_time,
                        "step": schedule.step,
                        "immediate": schedule.immediate,
                        "last_sync_datetime": schedule.last_sync_datetime.strftime("%Y-%m-%d %H:%M:%S") if schedule.last_sync_datetime else None,
                    }
                    result.append(schedule_dict)
                
                return result
        except Exception as e:
            print(f"获取任务调度列表时发生错误: {e}")
            return []

# 创建全局配置实例
config = Config()

    
