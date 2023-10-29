package com.zyc.common.entity;

public enum InstanceTypeEnum {

    LABEL("label","label","标签"),
    CROWD_RULE("crowd_rule","crowd_rule","客群规则"),
    CROWD_OPERATE("crowd_operate","crowd_operate","运算符"),
    DATA_NODE("data_node","data_node","数据节点"),
    CROWD_FILE("crowd_file","crowd_file","人群文件");


    private String value;
    private String code;
    private String desc;

    private InstanceTypeEnum(String code,String value,String desc) {
        this.code = code;
        this.value = value;
        this.desc = desc;
    }
    public String getValue() {
        return value;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
