package com.zyc.magic_mirror.label.calculate;

import com.zyc.magic_mirror.common.util.LogIdUtil;

public abstract class BaseProcessCalculate implements Runnable{

    @Override
    public void run() {
        LogIdUtil.generateAndSet();
        before();
        process();
        after();
        LogIdUtil.clear();
    }

    public abstract void before();

    public abstract void process();

    public abstract void after();
}
