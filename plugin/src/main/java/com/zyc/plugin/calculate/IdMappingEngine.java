package com.zyc.plugin.calculate;

import java.util.List;

/**
 * id_mapping 数据引擎
 */
public interface IdMappingEngine{

    /**
     * 获取id映射集合,每行通过逗号分割的字符串
     * @return
     */
    public List<String> get() throws Exception;

}
