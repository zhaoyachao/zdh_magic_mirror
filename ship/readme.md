#在线实时处理模块
    在线实时接入流量,分析用户命中的策略,并按策略中的经营方式经营用户

#最大的特色
    采用2个disruptor,自研master/worker模型实现高性能处理

#ship名称由来
    实时经营的用户就像水一样源源不断流进来,此平台是对用户做处理,所以就想到了船,船有很多个单词,选择ship作为名字,主要是听起来比较好听,之前也想过
    channel这个名字,由于不太好听就没使用

#应用场景
    1 实时接入用户流量,对用户做经营操作
    2 实时接入决策类的流量,比如风控,签到奖励等场景,根据规则判断用户满足的条件,做出对应的决策结果

#打包
    cd zdh_magic_mirror
    sh install.sh (此步必须执行,且需要修改install.sh脚本中指定的本地仓库路径)
    cd zdh_magic_mirror/ship
    sh build.sh

#启动
    cd zdh_magic_mirror/ship/xxx-SNAPSHOT
    sh bin/start.sh
    
#更新说明
    v1.0 修复树形图解析
    
    v5.1.3 实现disrptor模型
    v5.1.3 实现多种在线模块处理
    v5.2.0 实现小流量控制
    v5.2.1 优化三方依赖版本
    v5.2.3 优化执行流程
    v5.3.0 单独新增执行器实现
    v5.3.0 新增tn执行器
    v5.3.2 ship指定策略组经营/风控
    v5.3.3 ship修复部分执行器状态bug
    v5.3.4 ship新增plugin-kafka,plugin-http功能
    v5.3.4 ship标签执行器新增外部参数
    
    v5.6.4 删除fastjson
    
    v5.6.6 删除netty使用common包中的httpserver
    
    v1.0 增加暂停能力【未开发】
        