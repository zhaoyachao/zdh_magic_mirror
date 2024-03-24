package com.zyc.plugin.calculate.impl;

import com.google.common.collect.Maps;
import com.zyc.common.entity.FilterInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.FileUtil;
import com.zyc.plugin.calculate.FilterEngine;
import com.zyc.plugin.calculate.IdMappingEngine;
import scala.collection.immutable.Stream;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 本地文件过滤集
 * 适用于不变动的场景
 */
public class FileFilterEngineImpl implements FilterEngine {

    private String file_path;
    private FilterInfo filterInfo;

    public FileFilterEngineImpl(FilterInfo filterInfo, String file_path){
        this.filterInfo = filterInfo;
        this.file_path = file_path;
    }

    @Override
    public List<String> get() throws Exception {
        File f=new File(file_path);
        if(f.exists() && f.isFile()){
            return FileUtil.readStringSplit(f, Charset.forName("utf-8"), Const.FILE_STATUS_ALL);
        }
        return new ArrayList<>();
    }

    @Override
    public FilterResult getMap(Collection<String> rs) throws Exception {
        File f=new File(file_path);
        Map<String,String> id_map = Maps.newHashMap();
        Map<String,String> id_map_rs = Maps.newHashMap();
        Map<String,String> id_map_rs_error = Maps.newHashMap();
        FilterResult idMappingResult = new FilterResult();
        if(f.exists() && f.isFile()){
            List<String> id_mappings = FileUtil.readStringSplit(f, Charset.forName("utf-8"), Const.FILE_STATUS_ALL);
            for (String line:id_mappings){
                String[] idm = line.split(",",2);
                id_map.put(idm[0], idm[1]);
            }
        }

        for (String id: rs){
            if(!id_map.containsKey(id)){
                id_map_rs.put(id, "");
            }else{
                id_map_rs_error.put(id, this.filterInfo.getFilter_code());
            }
        }

        idMappingResult.setRs(id_map_rs);
        idMappingResult.setRs_error(id_map_rs_error);
        return idMappingResult;
    }
}
