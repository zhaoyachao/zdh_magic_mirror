package com.zyc.ship.entity;

public class LabelValueConfig {

    private String code;

    private String label_code;

    private String operate;

    private Object value;

    private String value_type;

    private String value_format;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getLabel_code() {
        return label_code;
    }

    public void setLabel_code(String label_code) {
        this.label_code = label_code;
    }

    public String getOperate() {
        return operate;
    }

    public void setOperate(String operate) {
        this.operate = operate;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getValue_type() {
        return value_type;
    }

    public void setValue_type(String value_type) {
        this.value_type = value_type;
    }

    public String getValue_format() {
        return value_format;
    }

    public void setValue_format(String value_format) {
        this.value_format = value_format;
    }
}
