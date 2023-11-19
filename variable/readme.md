# 变量/过滤服务
    变量服务主要能力是提供实时用户变量查询,用户变量主要分为个人属性,业务参数,预测模型等几类
    1：个人属性包括姓名,性别,年龄等
    2：业务参数包括公司业务产出的数据,比如游戏业务,是否玩过x游戏,游戏vip等级等
    3：预测模型是根据机器学习等算法产出的模型,比如常见的打分模型
    
# 变量存储数据结构
    redis做数据存储,以{场景}:{uid} 做一级key, value 为map结构, map的key为 变量code, map的value为2级map, 2级map的key为参数code, 2级map的value为参数value
    rocksdb作为历史数据存储,便于之后历史时刻数据问题追踪
# 过滤存储数据结构
    redis做数据存储,以{场景}:{uid} 做一级key, value 为map结构, map的key为 过滤code, map的value为2级map, 2级map的key为参数code, 2级map的value为参数value
    rocksdb作为历史数据存储,便于之后历史时刻数据问题追踪
    
#打包
    cd zdh_magic_mirror
    sh install.sh (此步必须执行,且需要修改install.sh脚本中指定的本地仓库路径)
    cd zdh_magic_mirror/variable
    sh build.sh
    
#启动
    cd zdh_magic_mirror/variable/xxx-SNAPSHOT
    sh bin/start.sh
    
#更新说明
    v5.2.1 优化三方依赖版本    