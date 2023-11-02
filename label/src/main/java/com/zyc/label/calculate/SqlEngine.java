package com.zyc.label.calculate;

/**
 * sql计算引擎表达式接口
 */
public interface SqlEngine {
    public String buildExpr(String param_value, String param_type, String param_code, String param_operate) throws Exception ;
}
