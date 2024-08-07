package com.zyc.plugin.calculate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
    public IdMappingResult getMap(Collection<String> rs) throws Exception;

    public static class IdMappingResult{
        public Map<String, String> rs;

        public Map<String, String> rs_error;

        public Map<String, String> getRs() {
            return rs;
        }

        public void setRs(Map<String, String> rs) {
            this.rs = rs;
        }

        public Map<String, String> getRs_error() {
            return rs_error;
        }

        public void setRs_error(Map<String, String> rs_error) {
            this.rs_error = rs_error;
        }
    }
}
