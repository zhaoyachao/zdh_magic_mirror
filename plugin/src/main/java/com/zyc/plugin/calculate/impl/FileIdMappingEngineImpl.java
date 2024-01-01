package com.zyc.plugin.calculate.impl;

import com.zyc.common.util.FileUtil;
import com.zyc.plugin.calculate.IdMappingEngine;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
            return FileUtil.readStringSplit(f, Charset.forName("utf-8"), "3");
        }
        return new ArrayList<>();
    }
}
