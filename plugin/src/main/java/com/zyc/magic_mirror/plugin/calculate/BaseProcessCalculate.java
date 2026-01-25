package com.zyc.magic_mirror.plugin.calculate;

import com.zyc.magic_mirror.common.util.LogIdUtil;

public abstract class BaseProcessCalculate implements Runnable{

    @Override
    public void run() {
        LogIdUtil.generateAndSet();
        before();
        try{
            process();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            after();
            LogIdUtil.clear();
        }
    }

    public abstract void before();

    public abstract void process();

    public abstract void after();
}
