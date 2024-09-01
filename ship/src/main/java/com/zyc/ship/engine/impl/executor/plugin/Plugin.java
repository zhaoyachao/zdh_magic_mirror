package com.zyc.ship.engine.impl.executor.plugin;

public interface Plugin {

    public String getName();
    public boolean execute() throws Exception;

}
