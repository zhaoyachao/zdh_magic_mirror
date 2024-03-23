package com.zyc.plugin.calculate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * filter 数据引擎
 */
public interface FilterEngine {

    /**
     * 获取过滤集合,每行通过逗号分割的字符串
     * @return
     */
    public List<String> get() throws Exception;

    /**
     * 根据输入集获取过滤结果
     * @param rs
     * @return
     * @throws Exception
     */
    public FilterResult getMap(Collection<String> rs) throws Exception;

    public static class FilterResult{
        public Map<String, String> rs;

        public Map<String, String> rs_error;//被过滤的信息,key,被过滤数据,value: 过滤集code

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
