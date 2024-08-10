package com.zyc.ship.engine.impl.excutor.plugin;

public interface Plugin {

    public String getName();
    public boolean execute() throws Exception;

}
