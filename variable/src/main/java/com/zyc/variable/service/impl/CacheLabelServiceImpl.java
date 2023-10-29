package com.zyc.variable.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.zyc.common.entity.LabelInfo;
import com.zyc.common.util.MybatisUtil;
import com.zyc.variable.dao.LabelMapper;
import com.zyc.variable.service.LabelService;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheLabelServiceImpl implements LabelService {

    public static Map<String,Map<String,Object>> cache=new HashMap<>();


    public void schedule(){
        //定时加载标签信息
        SqlSession sqlSession = null;
        try {
            sqlSession= MybatisUtil.getSqlSession();
            LabelMapper labelMapper = sqlSession.getMapper(LabelMapper.class);
            List<LabelInfo> rows = labelMapper.select();
            for(LabelInfo row : rows){

                String label_default = row.getLabel_default();
                if(!StringUtils.isEmpty(label_default)){
                    Map default_map = JSONObject.parseObject(label_default, Map.class);
                    cache.put(row.getLabel_code(), default_map);
                }else{
                    cache.put(row.getLabel_code(), null);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                sqlSession.close();
            }

        }
    }
}
