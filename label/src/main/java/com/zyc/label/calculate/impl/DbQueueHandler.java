package com.zyc.label.calculate.impl;

import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
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
            InstanceType.CROWD_RULE.getCode(),InstanceType.CUSTOM_LIST.getCode()};
    private String[] status = new String[]{Const.STATUS_CHECK_DEP_FINISH};

    @Override
    public Map<String,Object> handler() {
        try {
            if(config == null){
                throw new Exception("handler 未配置properties");
            }
            int slot_num = Integer.valueOf(config.getProperty("task.slot.total.num", "0"));
            String[] slot = config.getProperty("task.slot", "0").split(",");
            int start_slot =  Integer.valueOf(slot[0]);
            int end_slot =  Integer.valueOf(slot[1]);
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(status,instanceTypes);
            if(strategyInstances!=null && strategyInstances.size()>0){
                for (StrategyInstance strategyInstance: strategyInstances){
                    if(!NumberUtil.isLong(strategyInstance.getStrategy_id())){
                        LogUtil.error("",strategyInstance.getId(), "当前任务配置信息异常");
                        strategyInstance.setStatus(Const.STATUS_ERROR);
                        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
                        continue ;
                    }
                    if(Long.valueOf(strategyInstance.getStrategy_id())%slot_num >= start_slot && Long.valueOf(strategyInstance.getStrategy_id())%slot_num < end_slot){
                        return JSON.parseObject(JSON.toJSONString(strategyInstance), new TypeReference<Map<String, Object>>() {});
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
