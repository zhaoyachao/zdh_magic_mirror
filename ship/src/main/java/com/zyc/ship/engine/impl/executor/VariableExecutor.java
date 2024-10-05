package com.zyc.ship.engine.impl.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class VariableExecutor extends BaseExecutor{

    private static Logger logger= LoggerFactory.getLogger(VariableExecutor.class);

    public ShipResult execute(StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            JSONObject jsonObject = JSON.parseObject(strategyInstance.getRun_jsmind_data());



            //写入变量池
            String key = "varpool:logid:"+shipEvent.getLogGroupId();

            String varpool_code = jsonObject.getOrDefault("varpool_code","").toString();
            String varpool_operate = jsonObject.getOrDefault("varpool_operate","eq").toString();
            String varpool_type = jsonObject.getOrDefault("varpool_type","string").toString();
            String varpool_domain = jsonObject.getOrDefault("varpool_domain","").toString();
            String varpool_value = jsonObject.getOrDefault("varpool_value","").toString();
            String secondKey = varpool_domain+":"+varpool_code;

            Object value = JedisPoolUtil.redisClient().hGet(key, secondKey);

            if(value == null){
               throw new Exception("无法获取变量信息,变量池key:"+key+", 变量code:"+secondKey);
            }

            boolean ret = false;
            if(varpool_type.equalsIgnoreCase("string")){
                ret = diffStringValue(value.toString(),varpool_value,varpool_operate);
            }else if(varpool_type.equalsIgnoreCase("int")){
                ret = diffIntValue(Integer.valueOf(value.toString()),varpool_value,varpool_operate);
            }else if(varpool_type.equalsIgnoreCase("list")){
                throw new Exception("变量不支持的数据类型");
            }else if(varpool_type.equalsIgnoreCase("map")){
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
}
