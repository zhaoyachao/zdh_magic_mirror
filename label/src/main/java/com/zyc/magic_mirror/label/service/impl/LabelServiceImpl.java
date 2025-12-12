package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.LabelInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.label.dao.LabelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LabelServiceImpl extends BaseServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(LabelServiceImpl.class);

    public LabelInfo selectByCode(String label_code, String label_use_type){
        return executeReadOnly(sqlSession -> {
            LabelMapper labelMapper = sqlSession.getMapper(LabelMapper.class);
            LabelInfo labelInfo=new LabelInfo();
            labelInfo.setLabel_code(label_code);
            labelInfo.setIs_delete("0");
            labelInfo.setLabel_use_type(label_use_type);
            return labelMapper.selectByLabelCode(label_code, label_use_type);
        });
    }
}
