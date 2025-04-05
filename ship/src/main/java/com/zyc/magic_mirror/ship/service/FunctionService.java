package com.zyc.magic_mirror.ship.service;

import com.zyc.magic_mirror.common.entity.FunctionInfo;

import java.util.List;

public interface FunctionService {

    public List<FunctionInfo> selectAll();

    public FunctionInfo selectByFunctionCode(String function_code);

}
