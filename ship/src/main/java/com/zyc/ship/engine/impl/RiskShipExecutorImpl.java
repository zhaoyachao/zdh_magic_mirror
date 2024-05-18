package com.zyc.ship.engine.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipExecutor;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.excutor.*;
import com.zyc.ship.entity.ShipCommonInputParam;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RiskShipExecutorImpl implements ShipExecutor {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private ShipEvent shipEvent;

    private ShipResult shipResult;

    public RiskShipExecutorImpl(ShipEvent shipEvent, ShipResult shipResult){
        this.shipEvent = shipEvent;
        this.shipResult = shipResult;
    }

    @Override
    public ShipResult execute(StrategyInstance strategyInstance) {
        ShipResult shipResult1 = new RiskShipResultImpl();
        try{
            String tmp = ShipResultStatusEnum.CREATE.code;
            logger.info("executor: "+strategyInstance.getStrategy_context());
            shipResult1.setStrategyName(strategyInstance.getStrategy_context());
            String uid = ((ShipCommonInputParam)shipEvent.getInputParam()).getUid();
            String product_code = ((ShipCommonInputParam)shipEvent.getInputParam()).getProduct_code();
            String data_node = ((ShipCommonInputParam)shipEvent.getInputParam()).getData_node();
            String param = ((ShipCommonInputParam)shipEvent.getInputParam()).getParam();
            JSONObject jsonObjectParam = new JSONObject();

            if(!StringUtils.isEmpty(param) && JSONObject.parseObject(param).containsKey("user_param")){
                jsonObjectParam = JSONObject.parseObject(param).getJSONObject("user_param");
            }

            Map<String,Object> labelVaues = shipEvent.getLabelValues();
            String instance_type = strategyInstance.getInstance_type();
            JSONObject run_jsmind_data =  JSON.parseObject(strategyInstance.getRun_jsmind_data());
            if(instance_type.equalsIgnoreCase(InstanceType.LABEL.getCode())){
                LabelExecutor labelExecutor = new LabelExecutor();
                tmp =labelExecutor.execute(run_jsmind_data, labelVaues, uid, jsonObjectParam);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_RULE.getCode())){
                //不支持人群规则
                CrowdRuleExecutor crowdRuleExecutor = new CrowdRuleExecutor();
                tmp = crowdRuleExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_OPERATE.getCode())){
                //到执行器时的运算符,都是可执行的,master disruptor会提前判断
                CrowdOperateExecutor crowdOperateExecutor = new CrowdOperateExecutor();
                tmp = crowdOperateExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_FILE.getCode())){
                //不支持大批量人群文件
                CrowdFileExecutor crowdFileExecutor = new CrowdFileExecutor();
                tmp = crowdFileExecutor.execute(run_jsmind_data, product_code, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CUSTOM_LIST.getCode())){
                CustomListExecutor customListExecutor = new CustomListExecutor();
                tmp = customListExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.FILTER.getCode())){
                FilterExecutor filterExecutor = new FilterExecutor();
                tmp = filterExecutor.executor(run_jsmind_data, shipEvent, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.SHUNT.getCode())){
                ShuntExecutor shuntExecutor = new ShuntExecutor();
                tmp = shuntExecutor.execute(strategyInstance, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.TOUCH.getCode())){
                //触达
                tmp = ShipResultStatusEnum.ERROR.code;
            }else if(instance_type.equalsIgnoreCase(InstanceType.ID_MAPPING.getCode())){
                IdMappingExecutor idMappingExecutor = new IdMappingExecutor();
                tmp = idMappingExecutor.execute(run_jsmind_data, labelVaues, shipEvent, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.PLUGIN.getCode())){
                PluginExecutor pluginExecutor = new PluginExecutor();
                tmp = pluginExecutor.execute(run_jsmind_data, uid, strategyInstance);
            }else if(instance_type.equalsIgnoreCase(InstanceType.MANUAL_CONFIRM.getCode())){
                //不支持
                tmp = ShipResultStatusEnum.ERROR.code;
            }else if(instance_type.equalsIgnoreCase(InstanceType.RIGHTS.getCode())){
                RightsExecutor rightsExecutor = new RightsExecutor();
                tmp = rightsExecutor.execute(run_jsmind_data, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.CODE_BLOCK.getCode())){
                CodeBlockExecutor codeBlockExecutor = new CodeBlockExecutor();
                tmp = codeBlockExecutor.execute(run_jsmind_data, strategyInstance);
            }else if(instance_type.equalsIgnoreCase(InstanceType.DATA_NODE.getCode())){
                DataNodeExecutor dataNodeExecutor = new DataNodeExecutor();
                tmp = dataNodeExecutor.execute(run_jsmind_data, data_node, uid);
            }else if(instance_type.equalsIgnoreCase(InstanceType.RISK.getCode())){
                //决策信息
                RiskExecutor riskExecutor = new RiskExecutor();
                tmp = riskExecutor.execute(run_jsmind_data, shipResult1);
            }else if(instance_type.equalsIgnoreCase(InstanceType.TN.getCode())){
                //根据TN生成,任务(暂不支持,在线策略是否新增一个延迟插件)
                TnExecutor tnExecutor = new TnExecutor();
                tmp = tnExecutor.execute(run_jsmind_data, uid, strategyInstance, shipEvent);
            }else if(instance_type.equalsIgnoreCase(InstanceType.FUNCTION.getCode())){
                FunctionExecutor functionExecutor = new FunctionExecutor();
                tmp = functionExecutor.execute(run_jsmind_data, uid);
            }else{
                logger.error("暂不支持的经营类型: {}", instance_type);
                tmp = ShipResultStatusEnum.ERROR.code;
            }

            shipResult1.setStatus(tmp);
            return shipResult1;
        }catch (Exception e){
            e.printStackTrace();
        }

        return shipResult1;
    }





}
