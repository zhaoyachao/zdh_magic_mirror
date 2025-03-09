package com.zyc.plugin.calculate;

import com.zyc.common.entity.DataPipe;

import java.util.Set;

public class CalculateResult {

    private String file_dir;

    private Set<DataPipe> rs;

    private CalculateCommomParam calculateCommomParam;


    public CalculateResult(Builder builder){
        this.rs=builder.rs;
        this.calculateCommomParam=builder.calculateCommomParam;
        this.file_dir=builder.file_dir;
    }

    public String getFile_dir() {
        return file_dir;
    }

    public void setFile_dir(String file_dir) {
        this.file_dir = file_dir;
    }

    public Set<DataPipe> getRs() {
        return rs;
    }

    public void setRs(Set<DataPipe> rs) {
        this.rs = rs;
    }

    public CalculateCommomParam getCalculateCommomParam() {
        return calculateCommomParam;
    }

    public void setCalculateCommomParam(CalculateCommomParam calculateCommomParam) {
        this.calculateCommomParam = calculateCommomParam;
    }

    public static class Builder{
        private String file_dir;

        private Set<DataPipe> rs;

        private CalculateCommomParam calculateCommomParam;

        public Builder file_dir(String file_dir) {
            this.file_dir = file_dir;
            return this;
        }

        public Builder rs(Set<DataPipe> rs) {
            this.rs = rs;
            return this;
        }

        public Builder calculateCommomParam(CalculateCommomParam calculateCommomParam) {
            this.calculateCommomParam = calculateCommomParam;
            return this;
        }

        public CalculateResult build(){
            return new CalculateResult(this);
        }
    }
}
