package com.zyc.magic_mirror.plugin.calculate;

public abstract class BaseProcessCalculate implements Runnable{

    @Override
    public void run() {
        before();
        try{
            process();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            after();
        }


    }

    public abstract void before();

    public abstract void process();

    public abstract void after();
}
