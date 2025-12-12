package com.zyc.magic_mirror.label.service.impl;

import com.zyc.magic_mirror.common.entity.CustomerManagerInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.label.dao.CustomerManagerMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UserPoolServiceImpl extends BaseServiceImpl {
    private static Logger logger= LoggerFactory.getLogger(UserPoolServiceImpl.class);

    public List<CustomerManagerInfo> select(String product_code, String uid_type, String source){
        return executeReadOnly(sqlSession -> {
            CustomerManagerMapper customerManagerMapper = sqlSession.getMapper(CustomerManagerMapper.class);
            return customerManagerMapper.selectByUidTypeAndSource(product_code, uid_type, source);
        });
    }
}
