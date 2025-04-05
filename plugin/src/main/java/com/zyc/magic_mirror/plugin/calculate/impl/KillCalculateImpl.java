package com.zyc.magic_mirror.plugin.calculate.impl;

import cn.hutool.core.util.NumberUtil;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.common.util.ServerManagerUtil;
import com.zyc.magic_mirror.plugin.PluginServer;
import com.zyc.magic_mirror.plugin.calculate.KillCalculate;
import com.zyc.magic_mirror.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 监控任务杀死实现
 */
public class KillCalculateImpl extends BaseCalculate implements KillCalculate {
    private static Logger logger= LoggerFactory.getLogger(KillCalculateImpl.class);

    /**
     * {
     * 	"owner": "zyc",
     * 	"schedule_source": "2",
     * 	"strategy_context": "(年龄 in 19)",
     * 	"create_time": 1658629372000,
     * 	"jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *        },
     * 	"run_time": 1660993147000,
     * 	"group_instance_id": "1010624036146778112",
     * 	"cur_time": 1660993145000,
     * 	"pre_tasks": "",
     * 	"group_context": "测试策略组",
     * 	"priority": "",
     * 	"is_disenable": "false",
     * 	"is_delete": "0",
     * 	"run_jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *    },
     * 	"start_time": 1660993145000,
     * 	"update_time": 1660993147000,
     * 	"group_id": "测试策略组",
     * 	"misfire": "0",
     * 	"next_tasks": "1010624036201304064",
     * 	"id": "1010624036209692673",
     * 	"instance_type": "label",
     * 	"depend_level": "0",
     * 	"status": "create"
     * }
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public KillCalculateImpl(Map<String, Object> param, Properties dbConfig){
        this.param=param;
        this.dbConfig=new HashMap<>((Map)dbConfig);
    }

    @Override
    public void run() {
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        //唯一任务ID
        String logStr="";
        while (true){
            try{
                //获取要杀死的任务
                List<StrategyInstance> strategyInstanceList = strategyInstanceService.selectByStatus(new String[]{"kill"}, new String[]{"rights","filter","shunt","touch","id_mapping","plugin", "manual_confirm","code_block", "tn", "function"});

                String slotStr = ServerManagerUtil.getReportSlot("");
                String[] slots = slotStr.split(",");
                int slot_num = 100;
                int start_slot =  Integer.valueOf(slots[0]);
                int end_slot =  Integer.valueOf(slots[1]);

                if(strategyInstanceList != null && strategyInstanceList.size()>0){
                    for (StrategyInstance strategyInstance: strategyInstanceList){

                        if(!NumberUtil.isLong(strategyInstance.getStrategy_id())){
                            LogUtil.error("",strategyInstance.getId(), "当前任务配置信息异常");
                            strategyInstance.setStatus(Const.STATUS_ERROR);
                            strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
                            continue ;
                        }
                        if(!(Long.valueOf(strategyInstance.getStrategy_id())%slot_num +1 >= start_slot && Long.valueOf(strategyInstance.getStrategy_id())%slot_num + 1<= end_slot)){
                            continue;
                        }
                        //拉取任务future
                        Future future = PluginServer.tasks.getOrDefault(strategyInstance.getId(), null);
                        if(future != null && !future.isDone() && !future.isCancelled()){
                            boolean is_cannel = future.cancel(true);
                            if(is_cannel){
                                setStatus(strategyInstance.getId(), "killed");
                            }
                        }else{
                            setStatus(strategyInstance.getId(), "killed");
                        }
                        LogUtil.info(strategyInstance.getStrategy_id(), strategyInstance.getId(), "killed");
                    }
                }

                Thread.sleep(1000*10);

            }catch (InterruptedException e) {
                throw new RuntimeException(e);
            }catch (Exception e){
                //执行失败,更新标签任务失败
                logger.error("plugin kill run error: ", e);
            }finally {

            }
        }

    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return null;
    }
}
