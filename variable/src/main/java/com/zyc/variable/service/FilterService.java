package com.zyc.variable.service;

import java.util.Map;

public interface FilterService {

    public boolean isHit(String uid, String filter_code);

    public Map<String,String> getFilter(String uid);

}
