package com.zyc.ship.engine.impl;

import cn.hutool.core.date.DateUtil;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.JsonUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipExecutor;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.executor.*;
import com.zyc.ship.entity.ShipCommonInputParam;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class RiskShipExecutorImpl implements ShipExecutor {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private ShipEvent shipEvent;

    public RiskShipExecutorImpl(ShipEvent shipEvent){
        this.shipEvent = shipEvent;
    }

    @Override
    public ShipResult execute(StrategyInstance strategyInstance) {
        ShipResult shipResult1 = new RiskShipResultImpl();
        String startTime = String.valueOf(DateUtil.current());
        try{
            String tmp = ShipResultStatusEnum.CREATE.code;
            logger.info("executor: "+strategyInstance.getStrategy_context());
            String uid = ((ShipCommonInputParam)shipEvent.getInputParam()).getUid();
            String product_code = ((ShipCommonInputParam)shipEvent.getInputParam()).getProduct_code();
            String data_node = ((ShipCommonInputParam)shipEvent.getInputParam()).getData_node();
            String param = ((ShipCommonInputParam)shipEvent.getInputParam()).getParam();
            Map<String, Object> params = JsonUtil.toJavaMap(param);
            //jsonObjectParam 为自定义变量
            Map<String, Object> jsonObjectParam = new LinkedHashMap<>();

            if(!StringUtils.isEmpty(param) && params.containsKey("user_param")){
                jsonObjectParam = (Map<String, Object>)params.getOrDefault("user_param", new LinkedHashMap<>());
            }

            Map<String,Object> labelVaues = shipEvent.getLabelValues();
            String instance_type = strategyInstance.getInstance_type();
            Map<String, Object> run_jsmind_data =  JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());
            if(instance_type.equalsIgnoreCase(InstanceType.LABEL.getCode())){
                LabelExecutor labelExecutor = new LabelExecutor();
                shipResult1 =labelExecutor.execute(run_jsmind_data, labelVaues, uid, jsonObjectParam);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_RULE.getCode())){
                //不支持人群规则
                CrowdRuleExecutor crowdRuleExecutor = new CrowdRuleExecutor();
                shipResult1 = crowdRuleExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_OPERATE.getCode())){
                //到执行器时的运算符,都是可执行的,master disruptor会提前判断
                CrowdOperateExecutor crowdOperateExecutor = new CrowdOperateExecutor();
                shipResult1 = crowdOperateExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_FILE.getCode())){
                //不支持大批量人群文件
                CrowdFileExecutor crowdFileExecutor = new CrowdFileExecutor();
                shipResult1 = crowdFileExecutor.execute(run_jsmind_data, product_code, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CUSTOM_LIST.getCode())){
                CustomListExecutor customListExecutor = new CustomListExecutor();
                shipResult1 = customListExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.FILTER.getCode())){
                FilterExecutor filterExecutor = new FilterExecutor();
                shipResult1 = filterExecutor.executor(run_jsmind_data, shipEvent, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.SHUNT.getCode())){
                ShuntExecutor shuntExecutor = new ShuntExecutor();
                shipResult1 = shuntExecutor.execute(strategyInstance, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.TOUCH.getCode())){
                //触达
                tmp = ShipResultStatusEnum.ERROR.code;
            }else if(instance_type.equalsIgnoreCase(InstanceType.ID_MAPPING.getCode())){
                IdMappingExecutor idMappingExecutor = new IdMappingExecutor();
                shipResult1 = idMappingExecutor.execute(run_jsmind_data, labelVaues, shipEvent, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.PLUGIN.getCode())){
                PluginExecutor pluginExecutor = new PluginExecutor();
                shipResult1 = pluginExecutor.execute(run_jsmind_data, uid, strategyInstance, shipEvent, jsonObjectParam);
            }else if(instance_type.equalsIgnoreCase(InstanceType.MANUAL_CONFIRM.getCode())){
                //不支持
                tmp = ShipResultStatusEnum.ERROR.code;
                shipResult1.setStatus(tmp);
            }else if(instance_type.equalsIgnoreCase(InstanceType.RIGHTS.getCode())){
                RightsExecutor rightsExecutor = new RightsExecutor();
                shipResult1 = rightsExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CODE_BLOCK.getCode())){
                CodeBlockExecutor codeBlockExecutor = new CodeBlockExecutor();
                shipResult1 = codeBlockExecutor.execute(shipEvent, run_jsmind_data, strategyInstance);
            }else if(instance_type.equalsIgnoreCase(InstanceType.DATA_NODE.getCode())){
                DataNodeExecutor dataNodeExecutor = new DataNodeExecutor();
                shipResult1 = dataNodeExecutor.execute(run_jsmind_data, data_node, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.RISK.getCode())){
                //决策信息
                RiskExecutor riskExecutor = new RiskExecutor();
                shipResult1 = riskExecutor.execute(shipEvent, run_jsmind_data);
            }else if(instance_type.equalsIgnoreCase(InstanceType.TN.getCode())){
                //根据TN生成,任务(暂不支持,在线策略是否新增一个延迟插件)
                TnExecutor tnExecutor = new TnExecutor();
                shipResult1 = tnExecutor.execute(run_jsmind_data, uid, strategyInstance, shipEvent);
            }else if(instance_type.equalsIgnoreCase(InstanceType.FUNCTION.getCode())){
                FunctionExecutor functionExecutor = new FunctionExecutor();
                shipResult1 = functionExecutor.execute(shipEvent, run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.VARPOOL.getCode())){
                VarPoolExecutor varPoolExecutor = new VarPoolExecutor();
                shipResult1 = varPoolExecutor.execute(strategyInstance, shipEvent);
            }else if(instance_type.equalsIgnoreCase(InstanceType.VARIABLE.getCode())){
                VariableExecutor variableExecutor = new VariableExecutor();
                shipResult1 = variableExecutor.execute(strategyInstance, shipEvent, jsonObjectParam);
            }else{
                logger.error("暂不支持的经营类型: {}", instance_type);
                tmp = ShipResultStatusEnum.ERROR.code;
                shipResult1.setStatus(tmp);
            }
            String endTime = String.valueOf(DateUtil.current());
            shipResult1.setStartTime(startTime);
            shipResult1.setEndTime(endTime);
            shipResult1.setStrategyInstanceId(strategyInstance.getId());
            shipResult1.setStrategyName(strategyInstance.getStrategy_context());
            return shipResult1;
        }catch (Exception e){
            logger.error("ship exector execute error: ", e);
        }
        String endTime = String.valueOf(DateUtil.current());
        shipResult1.setStartTime(startTime);
        shipResult1.setEndTime(endTime);
        return shipResult1;
    }





}
