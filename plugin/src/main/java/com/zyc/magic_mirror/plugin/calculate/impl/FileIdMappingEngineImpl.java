package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.FileUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.plugin.calculate.IdMappingEngine;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 本地文件idmapping
 * 适用于不变动的场景
 */
public class FileIdMappingEngineImpl implements IdMappingEngine {

    private String file_path;
    private String id_mapping_code;

    public FileIdMappingEngineImpl(String file_path, String id_mapping_code){
        this.file_path = file_path;
        this.id_mapping_code = id_mapping_code;
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
    public IdMappingResult getMap(Collection<DataPipe> rs) throws Exception {

        if(!cn.hutool.core.io.FileUtil.exist(this.file_path)){
            cn.hutool.core.io.FileUtil.mkParentDirs(this.file_path);
            throw new Exception("未找到id_mapping依赖的文件: "+this.file_path);
        }
        File f=new File(file_path);
        Map<String,String> id_map = Maps.newHashMap();
        Set<DataPipe> id_map_rs = Sets.newHashSet();
        Set<DataPipe> id_map_rs_error = Sets.newHashSet();
        IdMappingResult idMappingResult = new IdMappingResult();
        if(f.exists() && f.isFile()){
            List<String> id_mappings = FileUtil.readTextFirstSplit(f, Charset.forName("utf-8"), ",");
            for (String line:id_mappings){
                String[] idm = line.split(",",2);
                id_map.put(idm[0], idm[1]);
            }
        }

        for (DataPipe r: rs){
            if(id_map.containsKey(r.getUdata())){
                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                stringObjectMap.put("mapping_data", r.getUdata()+","+id_map.get(r.getUdata()));
                r.setUdata(id_map.get(r.getUdata()));
                id_map_rs.add(r);
            }else{
                r.setStatus(Const.FILE_STATUS_FAIL);
                r.setStatus_desc("id mapping error");
                id_map_rs_error.add(r);
            }
        }

        idMappingResult.setRs(id_map_rs);
        idMappingResult.setRs_error(id_map_rs_error);
        return idMappingResult;
    }
}
