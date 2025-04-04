package com.zyc.variable.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zyc.common.http.HttpAction;
import com.zyc.variable.service.FilterService;
import com.zyc.variable.service.impl.FilterServiceImpl;

import java.util.List;
import java.util.Map;

/**
 * 过滤集
 */
public class FilterAction extends HttpAction {
    @Override
    public String getUri() {
        return "/api/v1/filter";
    }

    @Override
    public void before(Map<String, Object> param, String signKey) {

    }

    @Override
    public Object call(Map<String, Object> param) throws Exception {
        checkParam(param);

        String uid = param.get("uid").toString();
        String product_code = param.get("product_code").toString();
        FilterService filterService = new FilterServiceImpl();
        return filterService.getFilter(uid);
    }

    public void checkParam(Map<String, Object> param) throws Exception {
        Map<String, List<String>> rules = Maps.newHashMap();
        rules.put("uid", Lists.newArrayList("isEmpty"));
        rules.put("product_code", Lists.newArrayList("isEmpty"));
        check(rules, param);
    }


}
