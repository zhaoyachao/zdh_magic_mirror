package com.zyc.magic_mirror.ship.engine.impl.executor.plugin;

public interface Plugin {

    public String getName();
    public boolean execute() throws Exception;

}
