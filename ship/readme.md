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
    v1.0 增加暂停能力【未开发】
    v1.0 增加任务参数透传【未开发】
    v1.0 增加小流量控制【未开发】
        