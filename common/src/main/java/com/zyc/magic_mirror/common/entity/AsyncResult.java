package com.zyc.magic_mirror.common.entity;

import java.util.Set;

public class AsyncResult {

    private String task_id;

    private String task_ext;
    /**
     * fail, finish, running
     */
    private String status;

    /**
     * 错误码
     */
    private String code;

    /**
     * 中文信息
     */
    private String msg;

    /**
     * 结果下载地址
     */
    private String download_file_url;

    private Set<String> result;

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }

    public String getTask_ext() {
        return task_ext;
    }

    public void setTask_ext(String task_ext) {
        this.task_ext = task_ext;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Set<String> getResult() {
        return result;
    }

    public void setResult(Set<String> result) {
        this.result = result;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getDownload_file_url() {
        return download_file_url;
    }

    public void setDownload_file_url(String download_file_url) {
        this.download_file_url = download_file_url;
    }
}
