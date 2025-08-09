package com.zyc.magic_mirror.plugin.calculate.impl;

import cn.hutool.core.io.FileUtil;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.RocksDBUtil;
import com.zyc.magic_mirror.plugin.calculate.IdMappingEngine;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.*;

/**
 * rocksdb, idmapping
 * 适用于可变动的场景
 */
public class RocksDbIdMappingEngineImpl implements IdMappingEngine {

    private String file_path;
    private String id_mapping_code;

    public RocksDbIdMappingEngineImpl(String file_path, String id_mapping_code){
        this.file_path = file_path;
        this.id_mapping_code = id_mapping_code;
    }

    @Override
    public List<String> get() throws Exception {
        List<String> list = new ArrayList<>();
        RocksDB rocksDB = RocksDBUtil.getReadOnlyConnection(this.file_path);
        RocksIterator iterator = rocksDB.newIterator();
        for(iterator.seekToFirst();iterator.isValid();iterator.next()){
            String key = new String(iterator.key());
            String value = new String(iterator.value());
            list.add(key+","+value);
        }
        return list;
    }

    @Override
    public IdMappingResult getMap(Collection<DataPipe> rs) throws Exception {
        IdMappingResult idMappingResult = new IdMappingResult();
        Set<DataPipe> id_map_rs = Sets.newHashSet();
        Set<DataPipe> id_map_rs_error = Sets.newHashSet();
        if(!FileUtil.exist(this.file_path)){
            FileUtil.mkParentDirs(this.file_path);
            throw new Exception("未找到id_mapping依赖的文件: "+this.file_path);
        }
        RocksDB rocksDB = RocksDBUtil.getReadOnlyConnection(this.file_path);

        for (DataPipe r: rs){
            byte[] value = rocksDB.get(r.getUdata().getBytes());
            if(value != null && value.length>0){
                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(r.getExt());
                if(stringObjectMap.containsKey("mapping_data_"+id_mapping_code)){
                    String old = stringObjectMap.get("mapping_data_"+id_mapping_code).toString();
                    stringObjectMap.put("mapping_data_"+id_mapping_code, old+";"+r.getUdata()+","+new String(value));
                }else{
                    stringObjectMap.put("mapping_data_"+id_mapping_code, r.getUdata()+","+new String(value));
                }
                r.setUdata(new String(value));
                r.setExt(JsonUtil.formatJsonString(stringObjectMap));
                id_map_rs.add(r);
            } else{
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
