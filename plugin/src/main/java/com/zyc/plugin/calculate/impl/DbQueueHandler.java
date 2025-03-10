package com.zyc.plugin.calculate.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.NumberUtil;
import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.ServerManagerUtil;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
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

    public static String[] instanceTypes = new String[]{
            InstanceType.FILTER.getCode(),InstanceType.SHUNT.getCode(),InstanceType.TOUCH.getCode(),InstanceType.PLUGIN.getCode(),
            InstanceType.ID_MAPPING.getCode(),InstanceType.MANUAL_CONFIRM.getCode(),InstanceType.RIGHTS.getCode(),InstanceType.CODE_BLOCK.getCode(),
            InstanceType.TN.getCode(), InstanceType.FUNCTION.getCode(), InstanceType.VARPOOL.getCode(), InstanceType.VARIABLE.getCode()};
    private String[] status = new String[]{"check_dep_finish"};

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

            if(slot_num<=0){
                throw new Exception("服务槽位配置异常: "+slotStr);
            }
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(status,instanceTypes);
            if(strategyInstances!=null && strategyInstances.size()>0){
                for (StrategyInstance strategyInstance: strategyInstances){
                    if(!NumberUtil.isLong(strategyInstance.getStrategy_id())){
                        LogUtil.error("",strategyInstance.getId(), "当前任务配置信息异常");
                        strategyInstance.setStatus(Const.STATUS_ERROR);
                        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
                        continue ;
                    }
                    Map run_jsmind_data = JSON.parseObject(strategyInstance.getRun_jsmind_data(), Map.class);
                    String version_tag=run_jsmind_data.getOrDefault("version_tag","").toString();

                    //指定版本执行,判断当前实例是否可执行指定版本
                    if(!versionTag.equalsIgnoreCase(version_tag)){
                        continue;
                    }
                    if(Long.valueOf(strategyInstance.getStrategy_id())% slot_num + 1 >= start_slot && Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1 <= end_slot){
                        return BeanUtil.beanToMap(strategyInstance);
                        //return JSON.parseObject(JSON.toJSONString(strategyInstance), new TypeReference<Map<String, Object>>() {});
                }
                }

            }
           return null;
        } catch (Exception e) {
            logger.error("plugin handler db error: ", e);
        }
        return null;
    }

    @Override
    public void setProperties(Properties properties) {
        this.config = properties;
    }
}
