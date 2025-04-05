package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.FilterInfo;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.FileUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.plugin.calculate.FilterEngine;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

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
            return FileUtil.readTextFirstSplit(f, Charset.forName("utf-8"), ",");
        }
        return new ArrayList<>();
    }

    @Override
    public FilterResult getMap(Collection<DataPipe> rs) throws Exception {
        File f=new File(file_path);
        Map<String,String> id_map = Maps.newHashMap();
        Set<DataPipe> id_map_rs = Sets.newHashSet();
        Set<DataPipe> id_map_rs_error = Sets.newHashSet();
        FilterResult idMappingResult = new FilterResult();
        if(f.exists() && f.isFile()){
            List<String> id_mappings = FileUtil.readTextFirstSplit(f, Charset.forName("utf-8"), ",");
            for (String line:id_mappings){
                String[] idm = line.split(",",2);
                id_map.put(idm[0], idm[1]);
            }
        }

        for (DataPipe r: rs){
            if(!id_map.containsKey(r.getUdata())){
                id_map_rs.add(r);
            }else{
                r.setStatus(Const.FILE_STATUS_FAIL);
                r.setStatus_desc("hit filter "+this.filterInfo.getFilter_code());
                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                stringObjectMap.put("hit_filter", this.filterInfo.getFilter_code());
                id_map_rs_error.add(r);
            }
        }

        idMappingResult.setRs(id_map_rs);
        idMappingResult.setRs_error(id_map_rs_error);
        return idMappingResult;
    }
}
