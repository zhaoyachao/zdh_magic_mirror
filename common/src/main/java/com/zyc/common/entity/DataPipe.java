package com.zyc.common.entity;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.zyc.common.util.Const;
import com.zyc.common.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPipe {
    public static List<String> header = Lists.newArrayList("udata","status","status_desc","udata_type","task_type","execute_time","ext");

    private String udata;

    private String status;

    private String status_desc;

    private String udata_type;

    private String task_type;

    private String execute_time;

    private String ext;

    public DataPipe(){

    }

    public DataPipe(Builder builder){
        this.udata = builder.udata;
        this.status = builder.status;
        this.status_desc = builder.status_desc;
        this.udata_type = builder.udata_type;
        this.task_type = builder.task_type;
        this.execute_time = builder.execute_time==null?System.currentTimeMillis()/1000+"":builder.execute_time;
        this.ext = builder.ext==null?JsonUtil.formatJsonString(new HashMap<>()):JsonUtil.formatJsonString(builder.ext);

    }

    public String getUdata() {
        return udata;
    }

    public void setUdata(String udata) {
        this.udata = udata;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus_desc() {
        return status_desc;
    }

    public void setStatus_desc(String status_desc) {
        this.status_desc = status_desc;
    }

    public String getUdata_type() {
        return udata_type;
    }

    public void setUdata_type(String udata_type) {
        this.udata_type = udata_type;
    }

    public String getTask_type() {
        return task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }

    public String getExecute_time() {
        return execute_time;
    }

    public void setExecute_time(String execute_time) {
        this.execute_time = execute_time;
    }

    public String getExt() {
        return ext;
    }

    public void setExt(String ext) {
        this.ext = ext;
    }

    public String generateString(){
        return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s", udata==null?"":udata, status==null?"":status,status_desc==null?"":status_desc, udata_type==null?"":udata_type, task_type==null?"":task_type, execute_time==null?System.currentTimeMillis()/1000:execute_time, ext==null?"":ext);
    }

    public static List<DataPipe> readStringSplit(File file, Charset charset, String status, String split) throws IOException {
        List<DataPipe> result = new ArrayList<>();

        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            DataPipe dataPipe = DataPipe.readStringSplit(line, split);
            if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                result.add(dataPipe);
            }else{
                if(dataPipe.getStatus().equalsIgnoreCase(status)){
                    result.add(dataPipe);
                }
            }
        }
        return result;
    }

    public static DataPipe readStringSplit(String line, String split){
        Builder builder = new DataPipe.Builder();
        String[] cols = line.split(split);
        int size = cols.length;
        for(int i=0;i<size;i++){
            switch (header.get(i)){
                case "udata":
                    builder = builder.udata(cols[i]);
                    break;
                case "status":
                    builder = builder.status(cols[i]);
                    break;
                case "status_desc":
                    builder = builder.status_desc(cols[i]);
                    break;
                case "udata_type":
                    builder = builder.udata_type(cols[i]);
                    break;
                case "task_type":
                    builder = builder.task_type(cols[i]);
                    break;
                case "execute_time":
                    builder = builder.execute_time(cols[i]);
                    break;
                case "ext":
                    builder = builder.ext(JsonUtil.toJavaMap(cols[i]));
                    break;
                default:
                    break;
            }

        }
        return builder.build();
    }

    public static class Builder {

        private String udata;

        private String status;

        private String status_desc;

        private String udata_type;

        private String task_type;

        private String execute_time;

        private Map<String, Object> ext;

        public Builder udata(String udata){
            this.udata = udata;
            return this;
        }

        public Builder status(String status){
            this.status = status;
            return this;
        }

        public Builder status_desc(String status_desc){
            this.status_desc = status_desc;
            return this;
        }


        public Builder udata_type(String udata_type){
            this.udata_type = udata_type;
            return this;
        }

        public Builder task_type(String task_type){
            this.task_type = task_type;
            return this;
        }

        public Builder execute_time(String execute_time){
            this.execute_time = execute_time;
            return this;
        }

        public Builder ext(Map<String, Object> ext){
            this.ext = ext;
            return this;
        }

        public Builder ext(String key, String value){
            this.ext.put(key, value);
            return this;
        }

        public DataPipe build(){
            DataPipe dataPipe = new DataPipe(this);
            return dataPipe;
        }

    }
}
