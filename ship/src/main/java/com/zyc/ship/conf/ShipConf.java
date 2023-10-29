package com.zyc.ship.conf;

import java.util.Properties;

public class ShipConf {

    private static Properties properties=new Properties();

    public static String ON_LINE_MANAGER_SYNC="on_line_manager_sync";

    public static String ON_LINE_MANAGER_CORE_SIZE="on_line_manager_core_size";

    public ShipConf(Properties properties){
        this.properties = properties;
    }

    /**
     * 获取实时经营核心线程数
     * @return
     */
    public static int getOnLineManagerCoreSize(){
        return Integer.valueOf(properties.getProperty(ON_LINE_MANAGER_CORE_SIZE, "10"));
    }

    /**
     * 获取实时经营是否异步获取
     * @return
     */
    public static boolean getOnLineManagerSync(){
        return Boolean.valueOf(properties.getProperty(ON_LINE_MANAGER_SYNC, "false"));
    }
}
