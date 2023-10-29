package com.zyc.plugin.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.queue.QueueHandler;
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
    private StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();

    private String[] instanceTypes = new String[]{"filter","shunt","touch","id_mapping","plugin", "manual_confirm","rights", "code_block", "tn"};
    private String[] status = new String[]{"check_dep_finish"};

    @Override
    public Map<String,Object> handler() {
        try {
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByStatus(status,instanceTypes);
            if(strategyInstances!=null && strategyInstances.size()>0){
                return JSON.parseObject(JSON.toJSONString(strategyInstances.get(0)), new TypeReference<Map<String, Object>>() {
                });
            }
           return null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
