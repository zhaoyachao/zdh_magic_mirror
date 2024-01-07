package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;

public class DataNodeExecutor {

    public String execute(JSONObject run_jsmind_data, String data_node, String uid){
        String tmp = "error";
        try{
            //节点
            String s_data_node = run_jsmind_data.getOrDefault("data_node","").toString();
            if(data_node.equalsIgnoreCase(s_data_node)){
                tmp = "success";
            }
        }catch (Exception e){

        }
        return tmp;
    }
}
