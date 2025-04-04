package com.zyc.variable.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zyc.common.http.HttpAction;
import com.zyc.variable.service.VariableService;
import com.zyc.variable.service.impl.VariableServiceImpl;

import java.util.List;
import java.util.Map;

/**
 * 查询单个变量
 */
public class VariableUpdateAction extends HttpAction {
    @Override
    public String getUri() {
        return "/api/v1/variable/update";
    }

    @Override
    public void before(Map<String, Object> param, String signKey) {

    }

    @Override
    public Object call(Map<String, Object> param) throws Exception {
        checkParam(param);

        String uid = param.get("uid").toString();
        String variable = param.get("variable").toString();
        String product_code = param.get("product_code").toString();
        String value = param.get("value").toString();
        VariableService variableService = new VariableServiceImpl();
        return variableService.update(product_code, uid, variable, value);
    }

    public void checkParam(Map<String, Object> param) throws Exception {
        Map<String, List<String>> rules = Maps.newHashMap();
        rules.put("uid", Lists.newArrayList("isEmpty"));
        rules.put("variable", Lists.newArrayList("isEmpty"));
        rules.put("product_code", Lists.newArrayList("isEmpty"));
        check(rules, param);
    }


}
