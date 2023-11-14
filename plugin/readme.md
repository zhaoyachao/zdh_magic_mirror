# 标签插件平台
    标签计算平台,提供产出客群能力

# 打包
    cd zdh_magic_mirror
    sh install.sh (此步必须执行,且需要修改install.sh脚本中指定的本地仓库路径)
    cd zdh_magic_mirror/plugin
    sh build.sh

# 启动
    cd zdh_magic_mirror/plugin/xxx-SNAPSHOT
    sh bin/start.sh

# 更新记录
    v1.0 新增mybatis框架
    v1.0 默认使用数据库数据交互
    v1.0 数据计算引擎 只支持mysql
    v1.0 修复mybatis默认连接池,连接异常
    v1.0 实现过滤,分流,邮件推送,短信推送
    v1.0 增加实时执行结果日志-kafka实现
    
    v5.2.0 修复上下游数据关系解析bug
    v5.2.0 优化plugin,目前仅支持kafka,下个版本实现自动扩展