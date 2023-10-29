package com.zyc.ship.engine.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipExecutor;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.entity.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            String tmp = "create";
            logger.info("executor: "+strategyInstance.getStrategy_context());
            shipResult1.setStrategyName(strategyInstance.getStrategy_context());
            String uid = ((ShipCommonInputParam)shipEvent.getInputParam()).getUid();
            Map<String,Object> labelVaues = shipEvent.getLabelValues();
            String instance_type = strategyInstance.getInstance_type();
            if(instance_type.equalsIgnoreCase("label")){
                JSONObject jsonObject =  JSON.parseObject(strategyInstance.getRun_jsmind_data());
                List<LabelValueConfig> labelValueConfigs = labelParam2LableValueConfig(jsonObject);
                tmp = "success";
                for (LabelValueConfig labelValueConfig:labelValueConfigs){
                    if(!diffLable(labelValueConfig, labelVaues, uid)) {
                        tmp = "error";
                    }
                }
            }else if(instance_type.equalsIgnoreCase("data_node")){
                tmp = "success";
            }else if(instance_type.equalsIgnoreCase("filter")){
                JSONObject jsonObject =  JSON.parseObject(strategyInstance.getRun_jsmind_data());
                String[] filters = jsonObject.getString("filter").split(",");
                tmp = "success";
                if(!isHitFilter(filters, shipEvent.getFilterValues(), uid)){
                    tmp = "error";
                }
            }else if(instance_type.equalsIgnoreCase("shunt")){
                tmp = "success";
                //校验是否命中分流
                if(!shunt(null, strategyInstance, uid)){
                    tmp = "error";
                }
            }else if(instance_type.equalsIgnoreCase("risk")){
                //决策信息
                JSONObject jsonObject =  JSON.parseObject(strategyInstance.getRun_jsmind_data());
                String event_code = jsonObject.getString("rule_id");
                String event_code_result = jsonObject.getString("rule_param");
                tmp = "success";
                shipResult1.setRiskStrategyEventResult(new RiskStrategyEventResult(event_code, event_code_result));
            }else if(instance_type.equalsIgnoreCase("touch")){
                //触达
            }else if(instance_type.equalsIgnoreCase("plugin")){
                //插件
            }else if(instance_type.equalsIgnoreCase("id_mapping")){
                logger.warn("暂不支持id_mapping类型,默认成功");
                tmp = "error";
            }else{
                logger.error("暂不支持的经营类型: {}", instance_type);
                shipResult1.setStatus("error");
                tmp = "error";
            }

            shipResult1.setStatus(tmp);
            return shipResult1;
        }catch (Exception e){
            e.printStackTrace();
        }

        return shipResult1;
    }

    public List<LabelValueConfig> labelParam2LableValueConfig(JSONObject jsonObject){
        List<LabelValueConfig> labelValueConfigs = Lists.newArrayList();
        try{
            String rule_param = jsonObject.getString("rule_param");
            String rule_id = jsonObject.getString("rule_id");
            if(!StringUtils.isEmpty(rule_param)){
                List<StrategyLabelParamConfig> strategyLabelParamConfigs = JSON.parseArray(rule_param, StrategyLabelParamConfig.class);
                for (StrategyLabelParamConfig strategyLabelParamConfig:strategyLabelParamConfigs){
                    LabelValueConfig labelValueConfig=new LabelValueConfig();
                    labelValueConfig.setCode(strategyLabelParamConfig.getParam_code());
                    labelValueConfig.setValue(strategyLabelParamConfig.getParam_value());
                    labelValueConfig.setOperate(strategyLabelParamConfig.getParam_operate());
                    labelValueConfig.setValue_format("");
                    labelValueConfig.setValue_type(strategyLabelParamConfig.getParam_type());
                    labelValueConfig.setLabel_code(rule_id);
                    labelValueConfigs.add(labelValueConfig);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return labelValueConfigs;
    }

    /**
     * 标签计算
     * @param labelValueConfig
     * @param labelValues
     * @param uid
     * @return
     */
    public boolean diffLable(LabelValueConfig labelValueConfig, Map<String,Object> labelValues, String uid){
        if(diffValue(labelValues.get(labelValueConfig.getLabel_code()),labelValueConfig)){
            return true;
        }
        return false;
    }


    /**
     *
     * @param r 结果
     * @param labelValueConfig 用户配置参数
     * @return
     */
    public boolean diffValue(Object r,LabelValueConfig labelValueConfig){
        try{
            String code = labelValueConfig.getCode();//此处使用code获取对应的结果,原因可兼容一个标签有多个参数的场景
            String operate = labelValueConfig.getOperate();
            String value_type = labelValueConfig.getValue_type();
            Object value = labelValueConfig.getValue();
            if(r instanceof Map ){
                return diffValue(((Map) r).getOrDefault(code, null), value, value_type, operate);
            }else if(r instanceof String){
                return diffValue(JSONObject.parseObject(r.toString()).getOrDefault(code, null), value, value_type, operate);
            }
            return diffValue(r,value,value_type, operate);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param value_type
     * @param operate
     * @return
     */
    public boolean diffValue(Object lValue, Object uValue, String value_type, String operate){
        try{
            if(value_type.equalsIgnoreCase("int")){
                return diffIntValue(Integer.parseInt(lValue.toString()), uValue.toString(), operate);
            }else if(value_type.equalsIgnoreCase("double")){
                return diffDoubleValue(Double.parseDouble(lValue.toString()), uValue.toString(), operate);
            }else if(value_type.equalsIgnoreCase("long")){
                return diffLongValue(Long.parseLong(lValue.toString()),uValue.toString(),operate);
            }else if(value_type.equalsIgnoreCase("date")){
                return diffDateValue(lValue.toString(),uValue.toString(),operate);
            }else if(value_type.equalsIgnoreCase("string")){
                return diffStringValue(lValue.toString(),uValue.toString(),operate);
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffIntValue(Integer lValue, String uValue, String operate){
        try{

            if(operate.equalsIgnoreCase(">")){
                if(lValue>Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffDoubleValue(Double lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffLongValue(Long lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 日期类型 规定都是yyyy-MM-dd HH:mm:ss 格式
     * @param o1
     * @param t2
     * @param operate
     * @return
     */
    public boolean diffDateValue(String o1, String t2, String operate){
        try{
            if(operate.equalsIgnoreCase("relative_time")){
                //相对时间
                //param_value 结构[day|hour|second];3;4 ,表示相对未来3到4天
                //param_value 结构[day|hour|second];-3;-1 ,表示相对过去3天到1天
                Date cur = new Date();
                //todo 待实现相对时间逻辑
                throw new Exception("相对时间逻辑未实现");
            }
            long o = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(o1).getTime();
            long t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t2).getTime();
            if(operate.equalsIgnoreCase(">")){
                if(o>t) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(o<t) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(o>=t) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(o<=t) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(o==t) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(o!=t) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffStringValue(String lValue, String uValue, String operate){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase(">")){
                if(r<0) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(r>0) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(r<=0) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(r>=0) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(r==0) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(r!=0) return true;
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean isHitFilter(String[] filters, Map<String,Object> filterValues, String uid){
        if(filters==null || filters.length == 0){
            return true;
        }
        for (String filter: filters){
            if(filterValues.containsKey(filter)){
                return false;
            }
        }
        return true;
    }

    public boolean shunt(StrategyGroupInstance strategyGroup, StrategyInstance strategyInstance, String uid){
        try{
            JSONObject jsonObject = JSON.parseObject(strategyInstance.getRun_jsmind_data());
            String shunt_param_str=jsonObject.get("shunt_param").toString();
            List<Map> shunt_params = JSON.parseArray(shunt_param_str, Map.class);
            Map shunt_param = shunt_params.get(0);
            if(shunt_param == null){
                throw new Exception("分流信息为空");
            }
            String shunt_type = shunt_param.getOrDefault("shunt_type","num").toString();
            if(shunt_type.equalsIgnoreCase("hash")){
                //按hash一致性分流
                String[] shunt_values = shunt_param.getOrDefault("shunt_value","1;100").toString().split(";");
                int start = Integer.parseInt(shunt_values[0]);
                int end = Integer.parseInt(shunt_values[1]);
                int mod= Hashing.consistentHash(uid.hashCode(),100);
                if(mod>=start && mod <= end){
                    return true;
                }
                throw new Exception("hash分流期望值: "+shunt_param.getOrDefault("shunt_value","1;100").toString()+", 实际值: "+ mod);
            }else{
                //非hash分流统一返回false
                throw new Exception("不支持非hash方式之外的分流类型");
            }
        }catch (Exception e){
            logger.error("执行策略id: {}, 策略名称: {}, 策略类型: {}, 分流异常: ", strategyInstance.getId(), strategyInstance.getId(), strategyInstance.getInstance_type(), e.getCause());
            return false;
        }

    }
}
