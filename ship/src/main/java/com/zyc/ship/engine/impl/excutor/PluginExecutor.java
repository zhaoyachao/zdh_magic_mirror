package com.zyc.ship.engine.impl.excutor;

import com.alibaba.fastjson.JSONObject;

public class PluginExecutor {

    public String execute(JSONObject run_jsmind_data, String uid){
        String tmp = "error";
        try{
            //节点,当前不支持在线PLUGIN
            return tmp;
        }catch (Exception e){

        }
        return tmp;
    }
}
