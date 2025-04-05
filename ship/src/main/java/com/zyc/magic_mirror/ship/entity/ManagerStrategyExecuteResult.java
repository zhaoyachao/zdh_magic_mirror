package com.zyc.magic_mirror.ship.entity;

public class ManagerStrategyExecuteResult implements StrategyExecuteResult{

    private boolean isSuccess;

    public static ManagerStrategyExecuteResult build(boolean isSuccess){
        ManagerStrategyExecuteResult ser = new ManagerStrategyExecuteResult();
        ser.isSuccess(isSuccess);
        return ser;
    }

    public void isSuccess(boolean isSuccess){
        this.isSuccess = isSuccess;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }
}
