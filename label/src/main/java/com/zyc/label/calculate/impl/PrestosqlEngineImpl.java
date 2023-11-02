package com.zyc.label.calculate.impl;

import com.zyc.label.calculate.SqlEngine;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * presto 引擎表达式实现
 */
public class PrestosqlEngineImpl implements SqlEngine {

    @Override
    public String buildExpr(String param_value, String param_type, String param_code, String param_operate) throws Exception {
        throw new Exception("参数:"+param_code+"不支持的操作符");
    }

}
