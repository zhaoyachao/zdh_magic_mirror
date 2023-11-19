package com.zyc.plugin.calculate.impl;

import com.zyc.common.util.RocksDBUtil;
import com.zyc.plugin.calculate.IdMappingEngine;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;

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
}
