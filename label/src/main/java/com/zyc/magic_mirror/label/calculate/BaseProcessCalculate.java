package com.zyc.magic_mirror.label.calculate;

public abstract class BaseProcessCalculate implements Runnable{

    @Override
    public void run() {
        before();
        process();
        after();
    }

    public abstract void before();

    public abstract void process();

    public abstract void after();
}
