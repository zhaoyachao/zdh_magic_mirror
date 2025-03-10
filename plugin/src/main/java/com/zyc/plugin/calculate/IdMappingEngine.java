package com.zyc.plugin.calculate;

import com.zyc.common.entity.DataPipe;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * id_mapping 数据引擎
 */
public interface IdMappingEngine{

    /**
     * 获取id映射集合,每行通过逗号分割的字符串
     * @return
     */
    public List<String> get() throws Exception;

    /**
     * 根据输入集获取映射
     * @param rs
     * @return
     * @throws Exception
     */
    public IdMappingResult getMap(Collection<DataPipe> rs) throws Exception;

    public static class IdMappingResult{
        public Set<DataPipe> rs;

        public Set<DataPipe> rs_error;

        public Set<DataPipe> getRs() {
            return rs;
        }

        public void setRs(Set<DataPipe> rs) {
            this.rs = rs;
        }

        public Set<DataPipe> getRs_error() {
            return rs_error;
        }

        public void setRs_error(Set<DataPipe> rs_error) {
            this.rs_error = rs_error;
        }
    }
}
