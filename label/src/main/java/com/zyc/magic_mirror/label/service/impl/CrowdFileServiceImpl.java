package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.CrowdFileInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.label.dao.CrowdFileMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrowdFileServiceImpl extends BaseServiceImpl {

    private static Logger logger= LoggerFactory.getLogger(CrowdFileServiceImpl.class);

    public CrowdFileInfo selectById(String id){
        return executeReadOnly(sqlSession -> {
            CrowdFileMapper crowdFileMapper = sqlSession.getMapper(CrowdFileMapper.class);
            return crowdFileMapper.selectByPrimaryKey(id);
        });
    }
}
