# 脚本同步管理器(Script Sync Manager)
是一个用于同步脚本配置的工具,它可以根据配置文件中的字段,通过控制脚本的执行方式、参数输入、结果保存，以达到让用户专注于脚本和数据的目的， 同时提供了一些额外的功能，比如脚本配置管理、脚本依赖管理、脚本执行日志、脚本执行结果数据库保存、定时执行、错误快速重跑、追赶执行等功能。同时可以配合Menu.json去对数据和数据同步进行配置管理，是一款从数据管理角度出发的工具，而不仅是一个脚本执行工具。

## 配置说明
```json
{
        "name": "test",      // 脚本名称,必须唯一，也是数据库表名，也是data文件夹下的文件名，也是日志文件名、也是tools文件夹下的文件夹名，也是docs中的文件夹归类名称
        "cn_name": "测试专用",
        "desc": "一个用于测试的全量配置",
        "is_error_stop": false,  // 是否在脚本执行过程中发生错误时停止执行,默认false
        "save_to_db": false,  // 是否将脚本执行结果保存到script_sync_manager.db数据库,默认true
        "interval": "1",  // 如果type为iterator类型,则表示脚本执行间隔,单位为秒,默认1秒
        "type": "iterator-single",  // 脚本执行类型,默认iterator-single、iterator、single,这个分类和结果的处理有关，iterator-single遍历完依赖脚本后，再全量覆盖表数据,iterator表示每次执行完脚本后,将脚本结果添加到表中,single表示直接运行脚本，无需遍历依赖，返回的数据会全量覆盖表数据
        "schedule": {
            "period": "every_wDay",  // 脚本执行周期,默认every_wDay,表示每周执行一次,也可以设置为every_day,表示每天执行一次
            "func_name": "test",  // 脚本执行函数名,调用的是scripts文件夹下的test.py文件中的test函数,如果不写会根据type来判断，type为iterator-single或iterator时，默认调用iteration函数，type为single时，默认调用period函数
            "turn_on": true,  // 是否开启脚本执行,默认true
            "start_time": "21:34:00",  // 脚本执行开始时间,默认21:34:00
            "end_time": "23:59:59",  // 脚本执行结束时间,默认23:59:59，不写相当于start_time
            "step": "1s",  // 脚本执行时间间隔,默认1s,表示每1秒执行一次,1m,1h,0表示不间隔,直接在start_time执行一次
        }
    }
```

```json
{
    "name": "test",
    "cn_name": "测试专用",
    "desc": "一个用于测试的全量配置",
    "type": "iterator-single",
    "is_error_stop": false,
    "save_to_db": false,
    "interval": "1",
    "schedule": {
        "period": "every_wDay",
        "turn_on": true,
        "start_time": "21:34:00",
        "end_time": "23:59:59",
        "step": "1s",
        "immediate_run": true
}
```

### period
是自定义的一种执行周期,可以设置为every_wDay,表示每周执行一次,也可以设置为every_day,表示每天执行一次
- "every_day"：每天
- "every_day_6": 每6天
- "every_wDay"：每个工作日
- "every_week_1"：每周一
- "every_month_3"：每月3号
- "every_month3_L"：每年3月的最后一天
- "every_month3_15"：每年3月15号
- 直接时间格式："2024-01-01"直接返回该日期

### step
是脚本执行时间间隔,默认1s,表示每1秒执行一次,1m,1h,0表示不间隔,直接在start_time执行一次