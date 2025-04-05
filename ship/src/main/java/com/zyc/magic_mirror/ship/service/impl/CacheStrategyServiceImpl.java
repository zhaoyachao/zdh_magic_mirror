package com.zyc.magic_mirror.ship.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.StrategyInstance;
import com.zyc.magic_mirror.common.redis.JedisPoolUtil;
import com.zyc.magic_mirror.common.util.DAG;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.MybatisUtil;
import com.zyc.magic_mirror.ship.dao.StrategyGroupMapper;
import com.zyc.magic_mirror.ship.entity.StrategyGroupInstance;
import com.zyc.magic_mirror.ship.service.StrategyService;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class CacheStrategyServiceImpl implements StrategyService {

    private static Logger logger= LoggerFactory.getLogger(CacheStrategyServiceImpl.class);

    public static Map<String,List<StrategyGroupInstance>> cache=new HashMap<>();

    @Override
    public List<StrategyGroupInstance> selectBySceneAndDataNode(String scene, String data_node) {
        try {

            if(cache.containsKey(data_node)){
                return cache.get(data_node);
            }
            return null;
        } catch (Exception e) {
            logger.error("ship service selectBySceneAndDataNode error: ", e);
            return null;
        }finally {

        }
    }

    /**
     * {
     * "strategy_group_instance": [
     *        {
     * 		"id" : 1093130901082083328,
     * 		"group_context" : "测试在线",
     * 		"start_time" : "2023-04-05 11:11:54",
     * 		"end_time" : "2023-04-05 10:40:06",
     * 		"jsmind_data" : "{\"tasks\":[{\"more_task\":\"label\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"false\",\"id\":\"1093123017753497600\",\"operate\":\"and\",\"rule_id\":\"user_name\",\"rule_context\":\" (用户名 in admin)\",\"rule_param\":\"[{\\\"param_code\\\":\\\"user_name\\\",\\\"param_context\\\":\\\"用户名\\\",\\\"param_operate\\\":\\\"in\\\",\\\"param_value\\\":\\\"admin\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"rule_expression_cn\":\" (用户名 in admin)\",\"divId\":\"1093123017753497600\",\"name\":\"(and) (用户名 in admin)\",\"positionX\":301,\"positionY\":99,\"type\":\"label\"},{\"more_task\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"false\",\"id\":\"1093123106374946816\",\"operate\":\"and\",\"rule_id\":\"kafka\",\"rule_context\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"topic\\\",\\\"param_context\\\":\\\"\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"message\\\",\\\"param_context\\\":\\\"\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"rule_expression_cn\":\"kafka\",\"divId\":\"1093123106374946816\",\"name\":\"(and)kafka\",\"positionX\":412,\"positionY\":261,\"type\":\"plugin\"}],\"line\":[{\"connectionId\":\"con_12\",\"pageSourceId\":\"1093123017753497600\",\"pageTargetId\":\"1093123106374946816\"}]}",
     * 		"owner" : "zyc",
     * 		"is_delete" : "0",
     * 		"create_time" : "2023-04-05 11:11:56",
     * 		"update_time" : "2023-04-05 11:11:56",
     * 		"expr" : "",
     * 		"misfire" : "0",
     * 		"priority" : "",
     * 		"status" : "sub_task_dispatch",
     * 		"quartz_time" : null,
     * 		"use_quartz_time" : null,
     * 		"time_diff" : "",
     * 		"schedule_source" : "2",
     * 		"cur_time" : "2023-04-05 11:11:54",
     * 		"run_time" : "2023-04-05 11:11:56",
     * 		"run_jsmind_data" : "{\"run_data\":[{\"strategy_instance_id\":\"1093130901275021312\",\"divId\":\"1093123017753497600\"},{\"strategy_instance_id\":\"1093130901279215616\",\"divId\":\"1093123106374946816\"}],\"line\":[{\"pageTargetId\":\"1093123106374946816\",\"pageSourceId\":\"1093123017753497600\",\"connectionId\":\"con_12\"}],\"run_line\":[{\"from\":\"1093130901275021312\",\"to\":\"1093130901279215616\"}],\"tasks\":[{\"rule_expression_cn\":\" (用户名 in admin)\",\"rule_param\":\"[{\\\"param_code\\\":\\\"user_name\\\",\\\"param_context\\\":\\\"用户名\\\",\\\"param_operate\\\":\\\"in\\\",\\\"param_value\\\":\\\"admin\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"label\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\" (用户名 in admin)\",\"positionX\":301,\"rule_id\":\"user_name\",\"positionY\":99,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and) (用户名 in admin)\",\"more_task\":\"label\",\"id\":\"1093123017753497600\",\"divId\":\"1093123017753497600\"},{\"rule_expression_cn\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"topic\\\",\\\"param_context\\\":\\\"\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"message\\\",\\\"param_context\\\":\\\"\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"kafka\",\"positionX\":412,\"rule_id\":\"kafka\",\"positionY\":261,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)kafka\",\"more_task\":\"plugin\",\"id\":\"1093123106374946816\",\"divId\":\"1093123106374946816\"}]}",
     * 		"next_tasks" : null,
     * 		"pre_tasks" : null,
     * 		"strategy_group_id" : "1093123166152167424",
     * 		"group_type" : "online"
     *    }
     * ]}
     */
    public void schedule(){
        //定时加载策略信息
        SqlSession sqlSession = null;
        try {
            sqlSession= MybatisUtil.getSqlSession();
            StrategyGroupMapper strategyInstanceMappler = sqlSession.getMapper(StrategyGroupMapper.class);
            List<Map<String,Object>> rows = strategyInstanceMappler.select();
            Map<String,List<StrategyGroupInstance>> maps = Maps.newHashMap();
            for(Map<String,Object> row : rows){
                DAG dag=new DAG();
                StrategyGroupInstance strategyGroupInstance=new StrategyGroupInstance();
                String group_instance_id = row.get("id").toString();
                String group_id = row.get("strategy_group_id").toString();
                strategyGroupInstance.setData_node(Sets.newHashSet(row.getOrDefault("data_node", "-1").toString().split(",")));
                strategyGroupInstance.setId(group_instance_id);
                strategyGroupInstance.setGroup_id(group_id);

                Map<String, Object> run_jsmind_data = JsonUtil.toJavaMap(row.get("run_jsmind_data").toString());
                //获取跟节点
                List<Map<String, Object>> run_lins = (List<Map<String, Object>>)run_jsmind_data.get("run_line");
                List<Map<String,String>> dagMap = new ArrayList<>();
                for (Map<String,Object> run_line: run_lins){
                    String from = run_line.get("from").toString();
                    String to = run_line.get("to").toString();
                    Map<String,String> tmp = Maps.newHashMap();
                    tmp.put("from", from);
                    tmp.put("to", to);
                    dagMap.add(tmp);
                    dag.addEdge(from, to);
                }
                Set roots = dag.getSources();
                Set<String> data_nodes = Sets.newHashSet();
                Set<String> label_codes = Sets.newHashSet();
                strategyGroupInstance.setRoot_strategys(Sets.newHashSet(roots));
                List<Map<String,Object>> strategys = strategyInstanceMappler.selectStrategys(group_instance_id);
                Map<String,StrategyInstance> stringStrategyMap=new HashMap<>();
                for (Map<String,Object> stringObjectMap: strategys){
                    StrategyInstance strategyInstance=new StrategyInstance();
                    strategyInstance.setId(stringObjectMap.get("id").toString());
                    strategyInstance.setStrategy_context(stringObjectMap.get("strategy_context").toString());
                    strategyInstance.setData_node(stringObjectMap.get("data_node").toString());
                    strategyInstance.setGroup_id(strategyGroupInstance.getGroup_id());
                    strategyInstance.setGroup_instance_id(group_instance_id);
                    strategyInstance.setStrategy_id(stringObjectMap.get("strategy_id").toString());
                    strategyInstance.setInstance_type(stringObjectMap.get("instance_type").toString());
                    Map<String, Object> run_jsmind_data_strategy = JsonUtil.toJavaMap(stringObjectMap.get("run_jsmind_data").toString());
                    strategyInstance.setOperate(run_jsmind_data_strategy.get("operate").toString());
                    //strategyInstance.setParams(run_jsmind_data_strategy.getString("rule_param"));
                    strategyInstance.setRun_jsmind_data(stringObjectMap.get("run_jsmind_data").toString());
                    stringStrategyMap.put(strategyInstance.getId(), strategyInstance);
                    if(roots.contains(strategyInstance.getId())){
                        data_nodes.add(strategyInstance.getData_node());
                    }

                    //判断是否标签类型
                    if(stringObjectMap.get("instance_type").toString().equalsIgnoreCase("label")){
                        label_codes.add(run_jsmind_data_strategy.get("rule_id").toString());
                    }
                }
                strategyGroupInstance.setDag(dag);
                strategyGroupInstance.setDagMap(dagMap);
                strategyGroupInstance.setStrategyMap(stringStrategyMap);
                strategyGroupInstance.setLabel_codes(Lists.newArrayList(label_codes));
                Object small_flow_rate_map_str = JedisPoolUtil.redisClient.get("small_flow_rate_"+group_id);
                if(small_flow_rate_map_str != null && !StringUtils.isEmpty(small_flow_rate_map_str.toString())){
                    Map<String, String> stringStringMap = JsonUtil.toJavaBean(small_flow_rate_map_str.toString(), Map.class);
                    String small_flow_rate = stringStringMap.getOrDefault(group_instance_id,"");
                    strategyGroupInstance.setSmall_flow_rate(small_flow_rate);
                }

                for (String data_node: data_nodes){
                    if(maps.containsKey(data_node)){
                        maps.get(data_node).add(strategyGroupInstance);
                    }else{
                        maps.put(data_node, Lists.newArrayList(strategyGroupInstance));
                    }
                }
            }

            cache = maps;
            strategyInstanceMappler.update2Killed();
        } catch (IOException e) {
            logger.error("ship service schedule error: ", e);
        }finally {
            if(sqlSession != null){
                try {
                    sqlSession.getConnection().close();
                } catch (SQLException e) {
                    logger.error("ship service schedule sqlSession error: ", e);
                }
                sqlSession.close();
            }

        }
    }

}
