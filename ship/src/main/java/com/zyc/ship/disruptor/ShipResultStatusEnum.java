package com.zyc.ship.disruptor;

public enum ShipResultStatusEnum {

    CREATE("create", "新建"),
    WAIT("wait", "等待检查"),
    SUCCESS("success", "执行成功"),
    ERROR("error", "执行失败");

    public String code;

    public String desc;

    private ShipResultStatusEnum(String code, String desc){
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
