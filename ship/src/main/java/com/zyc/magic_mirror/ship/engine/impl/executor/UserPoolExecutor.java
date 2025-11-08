package com.zyc.magic_mirror.ship.engine.impl.executor;

import com.google.common.collect.Sets;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.disruptor.ShipEvent;
import com.zyc.magic_mirror.ship.disruptor.ShipResult;
import com.zyc.magic_mirror.ship.disruptor.ShipResultStatusEnum;
import com.zyc.magic_mirror.ship.engine.impl.RiskShipResultImpl;
import com.zyc.magic_mirror.ship.entity.ShipCommonInputParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class UserPoolExecutor extends BaseExecutor{
    private static Logger logger= LoggerFactory.getLogger(UserPoolExecutor.class);

    public ShipResult execute(Map<String, Object> run_jsmind_data, String uid, StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{

            String param = ((ShipCommonInputParam)shipEvent.getInputParam()).getParam();
            Map<String, Object> params = JsonUtil.toJavaMap(param);
            params.putAll(shipEvent.getRunParam());

            String product_code = run_jsmind_data.getOrDefault("product_code","").toString();
            String uid_type = run_jsmind_data.getOrDefault("uid_type","").toString();
            String source = run_jsmind_data.getOrDefault("source","").toString();

            String param_value_str = run_jsmind_data.getOrDefault("param_value","").toString();
            String param_code = run_jsmind_data.getOrDefault("param_code","").toString();
            String param_type = run_jsmind_data.getOrDefault("param_type","").toString();
            String param_operate = run_jsmind_data.getOrDefault("param_operate","").toString();

            Jinjava jinjava=new Jinjava();
            param_value_str = jinjava.render(param_value_str, params);

            boolean ret = expr(params, param_value_str, param_code, param_type, param_operate);
            String expr = param_code+", "+param_operate+", "+param_value_str;
            shipResult.addObj2Map("ret", ret);
            shipResult.addObj2Map("expr", expr);
            if(!ret){
                tmp = ShipResultStatusEnum.ERROR.code;
            }
        }catch (Exception e){
            logger.error("ship excutor userpool error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }


    private boolean expr(Map<String, Object> objectMap, String param_value, String param_code, String param_type, String param_operate){

        if(objectMap == null){
            return false;
        }
        boolean isRun = false;
        Object value = null;
        if(objectMap.containsKey(param_code)){
            isRun = true;
            value = objectMap.get(param_code);
        }

        if(!isRun){
            return false;
        }

        if(param_type.equalsIgnoreCase("string")){
            return diffStringValue(value.toString(), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("int")){
            return diffIntValue(Integer.valueOf(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("long")){
            return diffLongValue(Long.valueOf(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("decimal")){
            return diffDecimalValue(new BigDecimal(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("list")){
            //转json
            return diffListValue(JsonUtil.toJavaList(value.toString()), param_value, param_operate);
        }
        return false;
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
            if(operate.equalsIgnoreCase("gt")){
                if(lValue>Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue<Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue>=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue<=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.longValue() == Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
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
            }else if(operate.equalsIgnoreCase("like")){
                boolean in = uValue.contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffDecimalValue(BigDecimal lValue, String uValue, String operate){
        try{
            BigDecimal uValueDecimal = new BigDecimal(uValue);
            if(operate.equalsIgnoreCase("gt")){
                if(lValue.compareTo(uValueDecimal)>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue.compareTo(uValueDecimal)<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue.compareTo(uValueDecimal)>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue.compareTo(uValueDecimal)<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.compareTo(uValueDecimal)==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(lValue.compareTo(uValueDecimal)!=0) {
                    return true;
                }
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
