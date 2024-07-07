package com.zyc.ship.engine.impl.excutor;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import com.zyc.ship.entity.LabelValueConfig;
import com.zyc.ship.entity.StrategyLabelParamConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LabelExecutor {

    private static Logger logger= LoggerFactory.getLogger(LabelExecutor.class);
    /**
     *
     * @param run_jsmind_data
     * @param labelVaues
     * @param uid
     * @param user_param 用户自定义参数,从接口中传进来的参数
     * @return
     */
    public ShipResult execute(JSONObject run_jsmind_data, Map<String,Object> labelVaues, String uid, JSONObject user_param){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            List<LabelValueConfig> labelValueConfigs = labelParam2LableValueConfig(run_jsmind_data);
            for (LabelValueConfig labelValueConfig:labelValueConfigs){
                //此处遍历实现,默认仅支持多个参数and操作
                if(!diffLable(labelValueConfig, labelVaues, uid, user_param)) {
                    tmp = ShipResultStatusEnum.ERROR.code;
                    break;
                }
            }
        }catch (Exception e){
           logger.error("ship excutor label error: ", e);
           tmp = ShipResultStatusEnum.ERROR.code;
           shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }

    /**
     * 解析标签参数生成参数对象
     * 返回参数对象集合(标签有可能是多个参数,因此是一个集合)
     * @param jsonObject
     * @return
     */
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
            logger.error("ship label labelParam2LableValueConfig error: ", e);
        }
        return labelValueConfigs;
    }

    /**
     * 标签计算
     * 标签配置和标签值做对比运算
     * @param labelValueConfig 标签配置对象
     * @param labelValues 标签平台结果集
     * @param uid 用户id
     * @return
     */
    public boolean diffLable(LabelValueConfig labelValueConfig, Map<String,Object> labelValues, String uid, JSONObject user_param){
        if(diffValue(labelValues.get(labelValueConfig.getLabel_code()),labelValueConfig, user_param)){
            return true;
        }
        return false;
    }


    /**
     *
     * @param r 标签结果, map结构或者map类型的字符串
     * @param labelValueConfig 用户配置参数
     * @return
     */
    public boolean diffValue(Object r,LabelValueConfig labelValueConfig, JSONObject user_param){
        try{
            String code = labelValueConfig.getCode();//此处使用code获取对应的结果,原因可兼容一个标签有多个参数的场景
            String operate = labelValueConfig.getOperate();
            String value_type = labelValueConfig.getValue_type();
            Object value = labelValueConfig.getValue();
            //value 有可能是具体值,也可以是引用对象,引用对象参数$.abc
            if(value.toString().startsWith("$.")){
                String value_key = value.toString().substring(2);
                if(user_param.containsKey(value_key)){
                    value = user_param.get(value_key);
                }else{
                    value = "";
                }
            }
            if(r instanceof Map ){
                return diffValue(((Map) r).getOrDefault(code, null), value, value_type, operate);
            }else if(r instanceof String){
                return diffValue(JSONObject.parseObject(r.toString()).getOrDefault(code, null), value, value_type, operate);
            }
            return diffValue(r,value,value_type, operate);
        }catch (Exception e){
            logger.error("ship label diffValue error: ", e);
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
            }else if(value_type.equalsIgnoreCase("date") || value_type.equalsIgnoreCase("timestamp")){
                return diffDateValue(lValue.toString(),uValue.toString(),operate);
            }else if(value_type.equalsIgnoreCase("string")){
                return diffStringValue(lValue.toString(),uValue.toString(),operate);
            }else{
                return diffStringValue(lValue.toString(),uValue.toString(),operate);
            }
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
                if(lValue>Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.intValue() == Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.intValue() != Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
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
                if(lValue>Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.doubleValue() == Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.doubleValue() != Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
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
                if(lValue>Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.longValue() == Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.longValue() != Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 日期类型 规定都是yyyy-MM-dd HH:mm:ss 格式
     * @param lValue 标签结果
     * @param uValue 用户输入参数
     * @param operate
     * @return
     */
    public boolean diffDateValue(String lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("relative_time")){
                //相对时间
                //param_value 结构[day|hour|second];3;4 ,表示相对未来3到4天
                //param_value 结构[day|hour|second];-3;-1 ,表示相对过去3天到1天
                String[] uValueArray = uValue.split(";", 3);
                if(uValueArray.length != 3){
                    throw new Exception("相对时间参数格式不正确,格式[day|hour|second];[-]3;[-]4, 负号代表过去");
                }
                String unit = uValueArray[0];
                String start = uValueArray[1];
                String end = uValueArray[2];
                Date cur = new Date();
                long startTime = 0;
                long endTime = 0;
                if(unit.equalsIgnoreCase("day")){
                    startTime = DateUtil.offsetDay(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetDay(cur, Integer.valueOf(end)).getTime();
                }else if(unit.equalsIgnoreCase("hour")){
                    startTime = DateUtil.offsetHour(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetHour(cur, Integer.valueOf(end)).getTime();
                }else if(unit.equalsIgnoreCase("second")){
                    startTime = DateUtil.offsetSecond(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetSecond(cur, Integer.valueOf(end)).getTime();
                }
                long o = DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
                if(o >=startTime && o < endTime){
                    return true;
                }
                return false;
            }
            if(operate.equalsIgnoreCase("in")){
                long o =  DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
                long startTime = 0;
                long endTime = 0;
                String[] uValueArray = uValue.split(";", 2);
                String start = uValueArray[0];
                String end = uValueArray[1];
                startTime = DateUtil.parse(start, DatePattern.NORM_DATETIME_PATTERN).getTime();
                endTime = DateUtil.parse(end, DatePattern.NORM_DATETIME_PATTERN).getTime();
                if(o>=startTime && o < endTime){
                    return true;
                }
                return false;
            }
            long o = DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
            long t = DateUtil.parse(uValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
            if(operate.equalsIgnoreCase(">")){
                if(o>t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(o<t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(o>=t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(o<=t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(o==t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(o!=t) {
                    return true;
                }
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
                if(r<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(r>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(r<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(r>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(r==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(r!=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

}
