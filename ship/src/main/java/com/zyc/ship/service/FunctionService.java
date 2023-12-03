package com.zyc.ship.service;

import com.zyc.common.entity.FunctionInfo;

import java.util.List;

public interface FunctionService {

    public List<FunctionInfo> selectAll();

    public FunctionInfo selectByFunctionCode(String function_code);

}
