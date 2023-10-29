package com.zyc.ship.netty;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.util.HttpUtil;
import com.zyc.ship.common.Const;

public class HttpTest {

    public static void main(String[] args) throws Exception {

        JSONObject jsonObject=new JSONObject();
        jsonObject.put("uid", "zyc");
        jsonObject.put("id_type", "1");
        jsonObject.put("source", "test");
        jsonObject.put("scene", Const.ONLINE_MANAGER);
        jsonObject.put("data_node", "xxx");
        HttpUtil.postJSON("http://127.0.0.1:9002/api/v1/ship/accept",jsonObject.toJSONString());

    }
}
