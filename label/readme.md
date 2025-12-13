# 标签计算平台
    标签计算平台,提供产出客群能力
    
#tips
    运算符 and, or, not, not_use
    and: 交集运算符
    or: 并集运算符
    not: 排除运算符
    not_use: 解除上下游关系运算符
    
    任务的关系,and 和 or 都是针对上游任务的运算, 通过and/or(交集/并集)运算后的结果 和下游做 [交集] 运算
    not 是排除逻辑, 需先取上游的[并集]运算, 通过上游[并集运算结果] 排除下游数据

# 打包
    cd zdh_magic_mirror
    sh install.sh (此步必须执行,且需要修改install.sh脚本中指定的本地仓库路径)
    cd zdh_magic_mirror/label
    sh build.sh

# 启动
    cd zdh_magic_mirror/label/xxx-SNAPSHOT
    sh bin/start.sh

# 更新记录
    v1.0 新增mybatis框架
    v1.0 默认使用数据库数据交互
    v1.0 数据计算引擎 只支持mysql
    v1.0 修复mybatis默认连接池,连接异常
    v1.0 支持数据计算引擎配置化
    
    v5.2.1 优化三方依赖版本
    
    v5.2.2 统一 and,or,not,not_use操作符概念及处理逻辑
    
    v5.2.3 新增系统重启重置任务状态,新增redis主动关闭
    v5.2.3 增加优先级控制
    
    v5.3.0 label服务支持在线标签处理
    
    v5.3.2 新增sftp存储
    v5.3.2 新增任务分发策略
    v5.3.2 修复内存泄漏bug
    
    v5.3.6 优化sftp,新增minio存储
    
    v5.6.4 增加参数扩展,参数可层级传递
    v5.6.4 删除fastjson
    
    v5.6.7 优化代码
    
    v5.6.10 废弃异步重置状态队列,增加重试及再次检查机制
    v5.6.10 增加异步任务模式(具体使用需要下游任务提供接口)
    
    v5.6.16 删除启动脚本无用命令

    v5.6.19 支持tk.mybatis, 使用hikaricp连接池
    v5.6.19 优化代码
    
# 未实现待优化
    1.标签计算时-通过生成sql计算,当前sql语法兼容未做仅支持mysql, hive, presto, spark sql语法