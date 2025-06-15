package com.zyc.magic_mirror.common.entity;

import java.util.List;
import java.util.Set;

public class AsyncResult {

    private String task_id;

    private String task_ext;
    /**
     * fail, finish, running
     */
    private String status;

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
}
