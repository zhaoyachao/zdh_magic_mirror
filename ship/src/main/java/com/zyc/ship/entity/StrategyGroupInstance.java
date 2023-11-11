package com.zyc.ship.entity;


import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.DAG;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * 策略组任务实例信息
 * {
 * "strategy_group_instance": [
 *        {
 * 		"id" : 1090045415027380224,
 * 		"group_context" : "测试触达配置",
 * 		"start_time" : "2023-03-27 22:51:18",
 * 		"end_time" : "1970-01-01 08:00:00",
 * 		"jsmind_data" : "{\"tasks\":[{\"more_task\":\"label\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"true\",\"id\":\"1037135328067981312\",\"operate\":\"and\",\"rule_id\":\"tag_email\",\"rule_context\":\" (用户名 in zyc;admin)\",\"rule_param\":\"[{\\\"param_code\\\":\\\"user_name\\\",\\\"param_context\\\":\\\"用户名\\\",\\\"param_operate\\\":\\\"in\\\",\\\"param_value\\\":\\\"zyc;admin\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"rule_expression_cn\":\" (用户名 in zyc;admin)\",\"divId\":\"1037135328067981312\",\"name\":\"(and) (用户名 in zyc;admin)\",\"positionX\":209,\"positionY\":41,\"type\":\"label\"},{\"more_task\":\"touch\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"false\",\"id\":\"1037135416374857728\",\"operate\":\"and\",\"touch_context\":\"知心问候\",\"touch_id\":\"1\",\"touch_task\":\"email\",\"divId\":\"1037135416374857728\",\"name\":\"知心问候\",\"positionX\":238,\"positionY\":230,\"type\":\"touch\"}],\"line\":[{\"connectionId\":\"con_9\",\"pageSourceId\":\"1037135328067981312\",\"pageTargetId\":\"1037135416374857728\"}]}",
 * 		"owner" : "zyc",
 * 		"is_delete" : "0",
 * 		"create_time" : "2023-03-27 22:51:19",
 * 		"update_time" : "2023-03-27 22:52:11",
 * 		"expr" : "",
 * 		"misfire" : "0",
 * 		"priority" : "",
 * 		"status" : "finish",
 * 		"quartz_time" : null,
 * 		"use_quartz_time" : null,
 * 		"time_diff" : "",
 * 		"schedule_source" : "2",
 * 		"cur_time" : "2023-03-27 22:51:18",
 * 		"run_time" : "2023-03-27 22:51:19",
 * 		"run_jsmind_data" : "{\"run_data\":[{\"strategy_instance_id\":\"1090045415035768832\",\"divId\":\"1037135328067981312\"},{\"strategy_instance_id\":\"1090045415052546048\",\"divId\":\"1037135416374857728\"}],\"line\":[{\"pageTargetId\":\"1037135416374857728\",\"pageSourceId\":\"1037135328067981312\",\"connectionId\":\"con_9\"}],\"run_line\":[{\"from\":\"1090045415035768832\",\"to\":\"1090045415052546048\"}],\"tasks\":[{\"rule_expression_cn\":\" (用户名 in zyc;admin)\",\"rule_param\":\"[{\\\"param_code\\\":\\\"user_name\\\",\\\"param_context\\\":\\\"用户名\\\",\\\"param_operate\\\":\\\"in\\\",\\\"param_value\\\":\\\"zyc;admin\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"label\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\" (用户名 in zyc;admin)\",\"positionX\":209,\"rule_id\":\"tag_email\",\"positionY\":41,\"is_base\":\"true\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and) (用户名 in zyc;admin)\",\"more_task\":\"label\",\"id\":\"1037135328067981312\",\"divId\":\"1037135328067981312\"},{\"touch_context\":\"知心问候\",\"touch_task\":\"email\",\"type\":\"touch\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"positionX\":238,\"positionY\":230,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"知心问候\",\"more_task\":\"touch\",\"id\":\"1037135416374857728\",\"touch_id\":\"1\",\"divId\":\"1037135416374857728\"}]}",
 * 		"next_tasks" : null,
 * 		"pre_tasks" : null,
 * 		"strategy_group_id" : "1037135540547227648"
 *    }
 * ]}
 */
public class StrategyGroupInstance {

    private String id;
    //策略基础信息
    private String scene;

    private HashSet<String> data_node;

    private String system;

    private HashSet<String> root_strategys;

    private Map<String, StrategyInstance> strategyMap;//key: id, value: Strategy

    private Map<String,Object> labelValues;

    private Map<String,Object> filterValues;

    private DAG dag;

    private List<Map<String,String>> dagMap;

    private String group_id;

    private Map<String,String> runPath;

    private ShipCommonInputParam shipCommonInputParam;

    private List<String> label_codes;

    /**
     * 小流量比例
     */
    private String small_flow_rate;

    public String getScene() {
        return scene;
    }

    public void setScene(String scene) {
        this.scene = scene;
    }

    public HashSet<String> getData_node() {
        return data_node;
    }

    public void setData_node(HashSet<String> data_node) {
        this.data_node = data_node;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public HashSet<String> getRoot_strategys() {
        return root_strategys;
    }

    public void setRoot_strategys(HashSet<String> root_strategys) {
        this.root_strategys = root_strategys;
    }

    public Map<String, StrategyInstance> getStrategyMap() {
        return strategyMap;
    }

    public void setStrategyMap(Map<String, StrategyInstance> strategyMap) {
        this.strategyMap = strategyMap;
    }

    public Map<String, Object> getLabelValues() {
        return labelValues;
    }

    public void setLabelValues(Map<String, Object> labelValues) {
        this.labelValues = labelValues;
    }

    public Map<String, Object> getFilterValues() {
        return filterValues;
    }

    public void setFilterValues(Map<String, Object> filterValues) {
        this.filterValues = filterValues;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DAG getDag() {
        return dag;
    }

    public void setDag(DAG dag) {
        this.dag = dag;
    }

    public List<Map<String, String>> getDagMap() {
        return dagMap;
    }

    public void setDagMap(List<Map<String, String>> dagMap) {
        this.dagMap = dagMap;
    }

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public Map<String, String> getRunPath() {
        return runPath;
    }

    public void setRunPath(Map<String, String> runPath) {
        this.runPath = runPath;
    }

    public ShipCommonInputParam getShipCommonInputParam() {
        return shipCommonInputParam;
    }

    public void setShipCommonInputParam(ShipCommonInputParam shipCommonInputParam) {
        this.shipCommonInputParam = shipCommonInputParam;
    }

    public List<String> getLabel_codes() {
        return label_codes;
    }

    public void setLabel_codes(List<String> label_codes) {
        this.label_codes = label_codes;
    }

    public String getSmall_flow_rate() {
        return small_flow_rate;
    }

    public void setSmall_flow_rate(String small_flow_rate) {
        this.small_flow_rate = small_flow_rate;
    }
}
