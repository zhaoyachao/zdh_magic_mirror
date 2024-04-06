package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.hash.Hashing;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.entity.StrategyGroupInstance;

import java.util.List;
import java.util.Map;

public class ShuntExecutor {

    public String execute(StrategyInstance strategyInstance, String uid){
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            //校验是否命中分流
            if(!shunt(null, strategyInstance, uid)){
                tmp = ShipResultStatusEnum.ERROR.code;
            }
        }catch (Exception e){

        }
        return tmp;
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
            e.printStackTrace();
            return false;
        }

    }
}
