package com.zyc.magic_mirror.ship.disruptor;

public class ShipConst {

    public static String STATUS_ERROR = "error";

    public static String STATUS_CREATE = "create";

    public static String STATUS_WAIT = "wait";

    public static String STATUS_SUCCESS = "success";

    public static String DEPEND_LEVEL_SUCCESS = "0";//上游成功时触发
    public static String DEPEND_LEVEL_ERROR = "2";//上游失败时触发
    public static String DEPEND_LEVEL_FINISH = "3";//上游完成时触发

}
