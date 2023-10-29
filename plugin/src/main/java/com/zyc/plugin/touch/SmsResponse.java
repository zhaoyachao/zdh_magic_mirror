package com.zyc.plugin.touch;

public class SmsResponse {
    /**
     * ok:成功,其他均为失败
     */
    private String code;
    private String message;
    private Object object;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }
}
