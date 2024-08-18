package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.hash.Hashing;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.disruptor.ShipResultStatusEnum;
import com.zyc.ship.engine.impl.RiskShipResultImpl;
import com.zyc.ship.entity.StrategyGroupInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class VarPoolExecutor {

    private static Logger logger= LoggerFactory.getLogger(VarPoolExecutor.class);

    public ShipResult execute(StrategyInstance strategyInstance, ShipEvent shipEvent){
        ShipResult shipResult = new RiskShipResultImpl();
        String tmp = ShipResultStatusEnum.SUCCESS.code;
        try{
            JSONObject jsonObject = JSON.parseObject(strategyInstance.getRun_jsmind_data());
            String varpool_param_str=jsonObject.get("varpool_param").toString();
            List<Map> varpool_params = JSON.parseArray(varpool_param_str, Map.class);

            //写入变量池
            String key = "varpool:"+shipEvent.getLogGroupId();
            for (Map varpool: varpool_params){
                String varpool_code = varpool.getOrDefault("varpool_code","").toString();
                String varpool_domain = varpool.getOrDefault("varpool_domain","").toString();
                String varpool_value = varpool.getOrDefault("varpool_value","").toString();
                String secondKey = varpool_domain+":"+varpool_code;
                JedisPoolUtil.redisClient().hSet(key, secondKey, varpool_value);
            }

            //缓存5分钟
            if(JedisPoolUtil.redisClient().isExists(key)){
                JedisPoolUtil.redisClient().expire(key, 60*5L);
            }

        }catch (Exception e){
            logger.error("ship excutor shunt error: ", e);
            tmp = ShipResultStatusEnum.ERROR.code;
            shipResult.setMessage(e.getMessage());
        }
        shipResult.setStatus(tmp);
        return shipResult;
    }
}
