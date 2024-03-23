package com.zyc.plugin.calculate.impl;

import com.google.common.collect.Maps;
import com.zyc.common.util.RocksDBUtil;
import com.zyc.plugin.calculate.IdMappingEngine;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * rocksdb, idmapping
 * 适用于可变动的场景
 */
public class RocksDbIdMappingEngineImpl implements IdMappingEngine {

    private String file_path;

    public RocksDbIdMappingEngineImpl(String file_path){
        this.file_path = file_path;
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
    public IdMappingResult getMap(Collection<String> rs) throws Exception {
        IdMappingResult idMappingResult = new IdMappingResult();
        Map<String,String> id_map_rs = Maps.newHashMap();
        Map<String,String> id_map_rs_error = Maps.newHashMap();
        RocksDB rocksDB = RocksDBUtil.getReadOnlyConnection(this.file_path);

        for (String id: rs){
            byte[] value = rocksDB.get(id.getBytes());
            if(value != null && value.length>0){
                id_map_rs.put(id, new String(value));
            } else{
                id_map_rs_error.put(id, "");
            }
        }

        idMappingResult.setRs(id_map_rs);
        idMappingResult.setRs_error(id_map_rs_error);

        return idMappingResult;
    }
}
