package com.zyc.magic_mirror.variable.service.impl;

import com.zyc.magic_mirror.common.entity.LabelInfo;
import com.zyc.magic_mirror.common.service.impl.BaseServiceImpl;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.variable.dao.LabelMapper;
import com.zyc.magic_mirror.variable.service.LabelService;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheLabelServiceImpl extends BaseServiceImpl implements LabelService {

    public static Map<String,Map<String,Object>> cache=new HashMap<>();


    public void schedule(){
        //定时加载标签信息
        List<LabelInfo> rows = executeReadOnly(sqlSession -> {
            LabelMapper labelMapper = sqlSession.getMapper(LabelMapper.class);
            return labelMapper.selectList();
        });

        try {
            for(LabelInfo row : rows){

                String label_default = row.getLabel_default();
                if(!StringUtils.isEmpty(label_default)){
                    Map default_map = JsonUtil.toJavaBean(label_default, Map.class);
                    if(default_map != null){
                        cache.put(row.getLabel_code(), default_map);
                    }
                }else{
                    cache.put(row.getLabel_code(), null);
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
        }finally {

        }
    }
}
