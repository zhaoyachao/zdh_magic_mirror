package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;
import com.zyc.ship.disruptor.ShipResultStatusEnum;

public class DataNodeExecutor {

    public String execute(JSONObject run_jsmind_data, String data_node, String uid){
        String tmp = ShipResultStatusEnum.ERROR.code;
        try{
            //节点
            String s_data_node = run_jsmind_data.getOrDefault("data_node","").toString();
            if(data_node.equalsIgnoreCase(s_data_node)){
                tmp = ShipResultStatusEnum.SUCCESS.code;
            }
        }catch (Exception e){

        }
        return tmp;
    }
}
