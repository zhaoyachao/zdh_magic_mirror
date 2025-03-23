package com.zyc.ship.netty;

import com.zyc.common.util.HttpUtil;
import com.zyc.common.util.JsonUtil;
import com.zyc.ship.common.Const;

import java.util.HashMap;
import java.util.Map;

public class HttpTest {

    public static void main(String[] args) throws Exception {

        Map<String, Object> jsonObject=new HashMap<>();
        jsonObject.put("uid", "zyc");
        jsonObject.put("id_type", "1");
        jsonObject.put("source", "test");
        jsonObject.put("scene", Const.ONLINE_MANAGER);
        jsonObject.put("data_node", "xxx");
        HttpUtil.postJSON("http://127.0.0.1:9002/api/v1/ship/accept", JsonUtil.formatJsonString(jsonObject));

    }
}
