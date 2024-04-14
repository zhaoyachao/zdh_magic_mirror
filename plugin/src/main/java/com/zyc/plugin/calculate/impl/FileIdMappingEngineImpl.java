package com.zyc.plugin.calculate.impl;

import com.google.common.collect.Maps;
import com.zyc.common.util.Const;
import com.zyc.common.util.FileUtil;
import com.zyc.plugin.calculate.IdMappingEngine;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 本地文件idmapping
 * 适用于不变动的场景
 */
public class FileIdMappingEngineImpl implements IdMappingEngine {

    private String file_path;

    public FileIdMappingEngineImpl(String file_path){
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
    public IdMappingResult getMap(Collection<String> rs) throws Exception {

        if(!cn.hutool.core.io.FileUtil.exist(this.file_path)){
            cn.hutool.core.io.FileUtil.mkParentDirs(this.file_path);
            throw new Exception("未找到id_mapping依赖的文件: "+this.file_path);
        }
        File f=new File(file_path);
        Map<String,String> id_map = Maps.newHashMap();
        Map<String,String> id_map_rs = Maps.newHashMap();
        Map<String,String> id_map_rs_error = Maps.newHashMap();
        IdMappingResult idMappingResult = new IdMappingResult();
        if(f.exists() && f.isFile()){
            List<String> id_mappings = FileUtil.readStringSplit(f, Charset.forName("utf-8"), Const.FILE_STATUS_ALL);
            for (String line:id_mappings){
                String[] idm = line.split(",",2);
                id_map.put(idm[0], idm[1]);
            }
        }

        for (String id: rs){
            if(id_map.containsKey(id)){
                id_map_rs.put(id, id_map.get(id));
            }else{
                id_map_rs_error.put(id, "");
            }
        }

        idMappingResult.setRs(id_map_rs);
        idMappingResult.setRs_error(id_map_rs_error);
        return idMappingResult;
    }
}
