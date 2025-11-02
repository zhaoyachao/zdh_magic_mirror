package com.zyc.magic_mirror.label.calculate.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.NumberUtil;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.queue.QueueHandler;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.common.util.ServerManagerUtil;
import com.zyc.magic_mirror.label.service.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库方式获取数据实现
 */
public class DbQueueHandler implements QueueHandler {
    private static Logger logger= LoggerFactory.getLogger(DbQueueHandler.class);

    private Properties config;
    private StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();

    public static String[] instanceTypes = new String[]{InstanceType.LABEL.getCode(),InstanceType.CROWD_OPERATE.getCode(),InstanceType.CROWD_FILE.getCode(),
            InstanceType.CROWD_RULE.getCode(),InstanceType.CUSTOM_LIST.getCode(),InstanceType.USER_POOL.getCode()};
    private String[] status = new String[]{Const.STATUS_CHECK_DEP_FINISH};

    @Override
    public Map<String,Object> handler() {
        try {
            if(config == null){
                throw new Exception("handler 未配置properties");
            }

            String slotStr = ServerManagerUtil.getReportSlot("");
            String[] slots = slotStr.split(",");
            int slot_num = 100;
            int start_slot =  Integer.valueOf(slots[0]);
            int end_slot =  Integer.valueOf(slots[1]);

            String versionTag = ServerManagerUtil.getReportVersionTag("");

            if(slot_num!=100){
                throw new Exception("服务槽位配置异常: "+slotStr);
            }
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(status,instanceTypes);
            if(strategyInstances!=null && strategyInstances.size()>0){
                for (StrategyInstance strategyInstance: strategyInstances){
                    if(!NumberUtil.isLong(strategyInstance.getStrategy_id())){
                        LogUtil.error("",strategyInstance.getId(), "当前任务配置信息异常");
                        strategyInstance.setStatus(Const.STATUS_ERROR);
                        strategyInstanceService.updateStatusAndUpdateTimeById(strategyInstance);
                        continue ;
                    }
                    Map run_jsmind_data = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
                    String version_tag=run_jsmind_data.getOrDefault("version_tag","").toString();//版本标志,用于小流量测试

                    logger.info("db handler task: {}, data_version_tag: {}, data_slot: {},  server_version_tag: {}, server_slot: {}",strategyInstance.getStrategy_id(), version_tag, Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1, versionTag, slotStr);
                    //指定版本执行,判断当前实例是否可执行指定版本
                    if(!versionTag.equalsIgnoreCase(version_tag)){
                        continue;
                    }

                    if(Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1 >= start_slot && Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1 <= end_slot){
                        return BeanUtil.beanToMap(strategyInstance);
                    }
                }
            }
           return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void setProperties(Properties properties) {
        this.config = properties;
    }
}
