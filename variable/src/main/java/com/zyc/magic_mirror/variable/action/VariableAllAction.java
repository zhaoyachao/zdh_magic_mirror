package com.zyc.magic_mirror.variable.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zyc.magic_mirror.common.http.HttpAction;
import com.zyc.magic_mirror.variable.service.VariableService;
import com.zyc.magic_mirror.variable.service.impl.VariableServiceImpl;

import java.util.List;
import java.util.Map;

/**
 * 查询所有变量
 */
public class VariableAllAction extends HttpAction {
    @Override
    public String getUri() {
        return "/api/v1/all";
    }

    @Override
    public void before(Map<String, Object> param, String signKey) {

    }

    @Override
    public Object call(Map<String, Object> param) throws Exception {

        checkParam(param);

        String uid = param.get("uid").toString();

        VariableService variableService = new VariableServiceImpl();
        String product_code = param.get("product_code").toString();
        if(param.containsKey("variables")){
            List<String> variables = (List<String>)param.get("variables");
            return variableService.getMul(product_code, variables, uid);
        }
        return  variableService.getAll(product_code, uid);
    }

    public void checkParam(Map<String, Object> param) throws Exception {
        Map<String, List<String>> rules = Maps.newHashMap();
        rules.put("uid", Lists.newArrayList("isEmpty"));
        rules.put("product_code", Lists.newArrayList("isEmpty"));
        check(rules, param);
    }


}
