package com.zyc.magic_mirror.ship.conf;

import com.zyc.magic_mirror.common.util.ConfigUtil;

public class ShipConf {
    public static String ON_LINE_MANAGER_SYNC="on_line_manager_sync";

    public static String ON_LINE_MANAGER_CORE_SIZE="on_line_manager_core_size";

    public static String SHIP_DISRUPTOR_MASTER_HANDLER_NUM = "ship.disruptr.master.handler.num";
    public static String SHIP_DISRUPTOR_WORKER_HANDLER_NUM = "ship.disruptr.worker.handler.num";
    public static String SHIP_DISRUPTOR_MASTER_RING_NUM = "ship.disruptr.master.ring.num";
    public static String SHIP_DISRUPTOR_WORKER_RING_NUM = "ship.disruptr.worker.ring.num";

    public ShipConf(){
    }

    /**
     * 获取实时经营核心线程数
     * @return
     */
    public static int getOnLineManagerCoreSize(){
        return Integer.valueOf(ConfigUtil.get(ON_LINE_MANAGER_CORE_SIZE, "10"));
    }

    /**
     * 获取实时经营是否异步获取
     * @return
     */
    public static boolean getOnLineManagerSync(){
        return Boolean.valueOf(ConfigUtil.get(ON_LINE_MANAGER_SYNC, "false"));
    }

    public static String getProperty(String key){
        return ConfigUtil.get(key);
    }

    public static String getProperty(String key, String defaultValue){
        return ConfigUtil.get(key,defaultValue);
    }

    public static Object get(String key){
        return ConfigUtil.get(key);
    }

}
