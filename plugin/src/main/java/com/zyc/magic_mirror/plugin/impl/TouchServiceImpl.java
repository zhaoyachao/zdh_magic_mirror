package com.zyc.magic_mirror.plugin.impl;


import com.zyc.magic_mirror.common.entity.TouchConfigInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.plugin.dao.TouchMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TouchServiceImpl extends BaseServiceImpl {
    private Logger logger= LoggerFactory.getLogger(this.getClass());

    public TouchConfigInfo selectById(String id){

        return executeReadOnly(sqlSession -> {
            TouchMapper touchMapper = sqlSession.getMapper(TouchMapper.class);
            return touchMapper.selectByPrimaryKey(id);
        });
    }
}
