package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.groovy.GroovyFactory;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VariableExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(VariableExecutor.class);

    public ShipResult execute(StrategyInstance strategyInstance, ShipEvent shipEvent, Map<String, Object> userParam){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        Map<String, Object> ext = shipEvent.getRunParam();
        userParam.putAll(ext);

        try{
            Map<String, Object> jsonObject = JsonUtil.toJavaMap(strategyInstance.getRun_jsmind_data());

            String varpool_code = jsonObject.getOrDefault("varpool_code","").toString();
            String varpool_operate = jsonObject.getOrDefault("varpool_operate","eq").toString();
            String varpool_type = jsonObject.getOrDefault("varpool_type","string").toString();
            String varpool_domain = jsonObject.getOrDefault("varpool_domain","").toString();
            String varpool_value = jsonObject.getOrDefault("varpool_value","").toString();
            String varpool_expre = jsonObject.getOrDefault("varpool_expre","").toString();
            String secondKey = varpool_domain+":"+varpool_code;

            Object value = userParam.get(secondKey);

            if(value == null){
               throw new Exception("无法获取变量信息, 变量code:"+secondKey);
            }

            boolean ret = false;
            if(varpool_type.equalsIgnoreCase("string")||varpool_type.equalsIgnoreCase("decimal")){
                ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
            }else if(varpool_type.equalsIgnoreCase("int")){
                ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
            }else if(varpool_type.equalsIgnoreCase("long")){
                ret = diffLongValue(Long.valueOf(value.toString()),varpool_value,varpool_operate);
            }else if(varpool_type.equalsIgnoreCase("list")){
                //获取变量表达式
                if(!StringUtils.isEmpty(varpool_expre)){
                    Map<String, Object> parmas = new HashMap<>();
                    parmas.put("varpool_ret",  value);
                    value = GroovyFactory.execExpress(varpool_expre, parmas);
                }

                if(value instanceof String || value instanceof BigDecimal){
                    ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
                }else if(value instanceof Integer){
                    ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
                }else if(value instanceof Long){
                    ret = diffLongValue(Long.valueOf(value.toString()),varpool_value,varpool_operate);
                }else if(value instanceof List){
                    ret = diffListValue((List)value,varpool_value, varpool_operate);
                }else{
                    throw new Exception("不支持的数据类型");
                }

            }else if(varpool_type.equalsIgnoreCase("map")){
                //获取变量表达式
                if(!StringUtils.isEmpty(varpool_expre)){
                    Map<String, Object> parmas = new HashMap<>();
                    parmas.put("varpool_ret",  value);
                    value = GroovyFactory.execExpress(varpool_expre, parmas);
                }else{
                    value = JsonUtil.toJavaMap(value.toString());
                }

                if(value instanceof String || value instanceof BigDecimal){
                    ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
                }else if(value instanceof Integer){
                    ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
                }else if(value instanceof Long){
                    ret = diffLongValue(Long.valueOf(value.toString()),varpool_value,varpool_operate);
                }else if(value instanceof Map){
                    ret = diffMapValue((Map)value,varpool_value, varpool_operate);
                }else{
                    throw new Exception("不支持的数据类型");
                }
            }else{
                throw new Exception("变量不支持的数据类型");
            }

            if(!ret){
                throw new Exception("变量对比失败,实际值:"+value.toString()+", 期望值:"+varpool_value);
            }

        }catch (Exception e){
            logger.error("ship excutor shunt error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
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

            if(operate.equalsIgnoreCase("gt")){
                if(lValue>Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue<Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue>=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue<=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.intValue() == Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
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
     * @param lValue 变量值
     * @param uValue 用户配置的变量值
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

    public boolean diffStringValue(String lValue, String uValue, String operate){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase("gt")){
                if(r<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(r>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(r<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(r>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(r==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
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


    public boolean diffListValue(List<Object> lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.contains(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.stream().filter(s->s.toString().contains(uValue)).collect(Collectors.toList()).size()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffMapValue(Map lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.containsValue(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.values().stream().filter(s->s.toString().contains(uValue)).count()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }
}
