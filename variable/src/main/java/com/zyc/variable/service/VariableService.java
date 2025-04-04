package com.zyc.variable.service;

import java.util.List;
import java.util.Map;

public interface VariableService {

    public Object get(String product_code, String uid, String variable);

    public Map<String,String> getAll(String product_code, String uid);

    public Map<String,String> getMul(String product_code, List<String> variables, String uid);

    public Object update(String product_code, String uid, String variable, String value);

}
