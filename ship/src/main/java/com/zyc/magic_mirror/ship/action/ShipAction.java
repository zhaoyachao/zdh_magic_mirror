package com.zyc.magic_mirror.ship.action;

import com.google.common.collect.Maps;
import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.ship.entity.OutputParam;
import com.zyc.magic_mirror.ship.entity.ShipCommonInputParam;
import com.zyc.magic_mirror.ship.seaport.Input;
import com.zyc.magic_mirror.ship.seaport.impl.ShipInput;

import java.util.List;
import java.util.Map;

/**
 * 查询单个变量
 */
public class ShipAction extends HttpAction {
    @Override
    public String getUri() {
        return "/api/v1/ship/accept";
    }

    @Override
    public void before(Map<String, Object> param, String signKey) {

    }

    @Override
    public Object call(Map<String, Object> param) throws Exception {
        checkParam(param);

        String str= JsonUtil.formatJsonString(param);
        ShipCommonInputParam inputParam = JsonUtil.toJavaBean(str, ShipCommonInputParam.class);

        Input shipInput = new ShipInput();
        OutputParam outputParam = shipInput.accept(inputParam);
        return outputParam;
    }

    public void checkParam(Map<String, Object> param) throws Exception {
        Map<String, List<String>> rules = Maps.newHashMap();
//        rules.put("uid", Lists.newArrayList("isEmpty"));
//        rules.put("variable", Lists.newArrayList("isEmpty"));
//        rules.put("product_code", Lists.newArrayList("isEmpty"));
        check(rules, param);
    }


}
