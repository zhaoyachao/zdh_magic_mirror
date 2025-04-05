package com.zyc.magic_mirror.plugin.touch;

import com.zyc.magic_mirror.common.entity.TouchConfigInfo;

import java.util.Map;

public interface EmailTouch {

    public void init(Map<String,Object> param, TouchConfigInfo touchConfigInfo);

    public String send(String account);

    public Map<String,String> getDynamicParamByUser(String account, String touchParamCodes);
}
