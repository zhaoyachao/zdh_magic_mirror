package com.zyc.magic_mirror.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.CustomerManagerInfo;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.label.service.impl.UserPoolServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 用户池实现
 */
public class UserPoolCalculateImpl extends BaseCalculate implements Runnable {
    private static Logger logger= LoggerFactory.getLogger(UserPoolCalculateImpl.class);

    /**
     * {
     * "strategy_instance": [
     *        {
     * 		"id" : 1434224944966799369,
     * 		"strategy_context" : "(and)选择性别男性的用户",
     * 		"group_id" : "1034976263502041088",
     * 		"group_context" : "测试divid优化",
     * 		"group_instance_id" : "1434224944819998720",
     * 		"instance_type" : "user_pool",
     * 		"start_time" : "2025-11-01 16:57:52",
     * 		"end_time" : "2025-04-28 00:00:00",
     * 		"jsmind_data" : "{\"more_task\":\"user_pool\",\"is_disenable\":\"false\",\"depend_level\":\"0\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"false\",\"data_status\":\"1\",\"project_code\":\"\",\"project_scene\":\"\",\"version_tag\":\"\",\"is_async\":\"false\",\"plan_retry_count\":\"0\",\"id\":\"1434221738161344512\",\"operate\":\"and\",\"rule_id\":\"1434223437512970240\",\"rule_context\":\"选择性别男性的用户\",\"param_code\":\"sex\",\"param_type\":\"string\",\"param_operate\":\"eq\",\"param_value\":\"man\",\"product_code\":\"zdh\",\"uid_type\":\"account\",\"source\":\"test1\",\"divId\":\"1434221738161344512\",\"name\":\"(and)选择性别男性的用户\",\"positionX\":28,\"positionY\":29,\"type\":\"user_pool\"}",
     * 		"owner" : "zyc",
     * 		"is_delete" : "0",
     * 		"create_time" : "2025-11-01 16:57:53",
     * 		"update_time" : "2025-11-01 16:57:53",
     * 		"expr" : "1d",
     * 		"misfire" : "0",
     * 		"priority" : "",
     * 		"status" : "check_dep_finish",
     * 		"quartz_time" : "2025-01-08 00:31:01",
     * 		"use_quartz_time" : "on",
     * 		"time_diff" : "50",
     * 		"schedule_source" : "2",
     * 		"cur_time" : "2025-11-01 16:57:52",
     * 		"run_time" : "2025-11-01 16:57:53",
     * 		"run_jsmind_data" : "{\"more_task\":\"user_pool\",\"is_disenable\":\"false\",\"depend_level\":\"0\",\"time_out\":\"86400\",\"touch_type\":\"database\",\"is_base\":\"false\",\"data_status\":\"1\",\"project_code\":\"\",\"project_scene\":\"\",\"version_tag\":\"\",\"is_async\":\"false\",\"plan_retry_count\":\"0\",\"id\":\"1434221738161344512\",\"operate\":\"and\",\"rule_id\":\"1434223437512970240\",\"rule_context\":\"选择性别男性的用户\",\"param_code\":\"sex\",\"param_type\":\"string\",\"param_operate\":\"eq\",\"param_value\":\"man\",\"product_code\":\"zdh\",\"uid_type\":\"account\",\"source\":\"test1\",\"divId\":\"1434221738161344512\",\"name\":\"(and)选择性别男性的用户\",\"positionX\":28,\"positionY\":29,\"type\":\"user_pool\"}",
     * 		"next_tasks" : "1434224944966799361",
     * 		"pre_tasks" : "",
     * 		"is_disenable" : "false",
     * 		"depend_level" : "0",
     * 		"touch_type" : "database",
     * 		"strategy_id" : "1434221738161344512",
     * 		"group_type" : "offline",
     * 		"data_node" : "",
     * 		"is_notice" : null
     *    }
     * ]}
     * @param param
     * @param atomicInteger
     * @param dbConfig
     */

    public UserPoolCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        super(param, atomicInteger, dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
    }

    @Override
    public String storageMode() {
        return this.dbConfig.getOrDefault("storage.mode", "");
    }

    @Override
    public String getBucket() {
        return this.dbConfig.getOrDefault("storage.minio.bucket", super.getBucket());
    }

    @Override
    public String getRegion() {
        return this.dbConfig.getOrDefault("storage.minio.region", super.getRegion());
    }

    @Override
    public void process() {
        String logStr="";
        try{

            //获取标签code
            String rule_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            Set<DataPipe> cur_rows = Sets.newHashSet();
            Set<String> rowsStr = Sets.newHashSet();
            //判断是否跳过类的策略,通过is_disenable=true,禁用的任务直接拉取上游任务的结果,并集(),交集(),排除()
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //生成参数
                String product_code = run_jsmind_data.getOrDefault("product_code","").toString();
                String uid_type = run_jsmind_data.getOrDefault("uid_type","").toString();
                String source = run_jsmind_data.getOrDefault("source","").toString();

                String param_value_str = run_jsmind_data.getOrDefault("param_value","").toString();
                String param_code = run_jsmind_data.getOrDefault("param_code","").toString();
                String param_type = run_jsmind_data.getOrDefault("param_type","").toString();
                String param_operate = run_jsmind_data.getOrDefault("param_operate","").toString();

                Map<String, Object> commonParam = getJinJavaCommonParam();
                Jinjava jinjava = new Jinjava();
                param_value_str = jinjava.render(param_value_str, commonParam);

                logStr = StrUtil.format("task: {}, param_code: {}, param_operate: {}, param_value: {}", strategyLogInfo.getStrategy_instance_id(), param_code, param_operate, param_value_str);
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);

                UserPoolServiceImpl userPoolService = new UserPoolServiceImpl();
                List<CustomerManagerInfo> customerManagerInfos = userPoolService.select(product_code, uid_type, source);

                //遍历customer
                for(CustomerManagerInfo customerManagerInfo: customerManagerInfos){
                    try{
                        List<Map<String, Object>> objectMaps = JsonUtil.toJavaListMap(customerManagerInfo.getConfig());
                        boolean result = expr(objectMaps, param_value_str, param_code, param_type, param_operate);
                        if(result){
                            rowsStr.add(customerManagerInfo.getUid());
                        }
                    }catch (Exception e){
                        logger.error("label userpool expr run error: ", e);
                        logStr = StrUtil.format("task: {}, error: {}", strategyLogInfo.getStrategy_instance_id(), e.getMessage());
                        LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    }
                }

                cur_rows = rowsStr.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(new HashMap<>()).build()).collect(Collectors.toSet());

            }

            Set<DataPipe> rs=Sets.newHashSet();
            String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                    strategyLogInfo.getStrategy_group_instance_id());
            //解析上游任务并和当前节点数据做运算
            rs = calculateCommon(strategyLogInfo, "offline",cur_rows, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);

        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("label userpool run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }


    private boolean expr(List<Map<String, Object>> objectMaps, String param_value, String param_code, String param_type, String param_operate){

        if(objectMaps == null){
            return false;
        }
        boolean isRun = false;
        Object value = null;
        for (Map<String, Object> objectMap: objectMaps){
            if(objectMap.get("param_code").equals(param_code)){
                isRun = true;
                value = objectMap.get("param_value");
                break;
            }
        }

        if(!isRun){
            return false;
        }

        if(param_type.equalsIgnoreCase("string")){
            return diffStringValue(value.toString(), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("int")){
            return diffIntValue(Integer.valueOf(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("long")){
            return diffLongValue(Long.valueOf(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("decimal")){
            return diffDecimalValue(new BigDecimal(value.toString()), param_value, param_operate);
        }else if(param_type.equalsIgnoreCase("list")){
            //转json
            return diffListValue(JsonUtil.toJavaList(value.toString()), param_value, param_operate);
        }
        return false;
    }


    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffIntValue(Integer lValue, String uValue, String operate){
        try{

            if(operate.equalsIgnoreCase("gt")){
                if(lValue>Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue<Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue>=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue<=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.intValue() == Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(lValue.intValue() != Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }


    /**
     *
     * @param lValue 变量值
     * @param uValue 用户配置的变量值
     * @param operate
     * @return
     */
    public boolean diffLongValue(Long lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("gt")){
                if(lValue>Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue<Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue>=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue<=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.longValue() == Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(lValue.longValue() != Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffStringValue(String lValue, String uValue, String operate){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase("gt")){
                if(r<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(r>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(r<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(r>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(r==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(r!=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }else if(operate.equalsIgnoreCase("like")){
                boolean in = uValue.contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffDecimalValue(BigDecimal lValue, String uValue, String operate){
        try{
            BigDecimal uValueDecimal = new BigDecimal(uValue);
            if(operate.equalsIgnoreCase("gt")){
                if(lValue.compareTo(uValueDecimal)>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lt")){
                if(lValue.compareTo(uValueDecimal)<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("gte")){
                if(lValue.compareTo(uValueDecimal)>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("lte")){
                if(lValue.compareTo(uValueDecimal)<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("eq")){
                if(lValue.compareTo(uValueDecimal)==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("neq")){
                if(lValue.compareTo(uValueDecimal)!=0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffListValue(List<Object> lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.contains(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.stream().filter(s->s.toString().contains(uValue)).collect(Collectors.toList()).size()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffMapValue(Map lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("in")){
                if(lValue.containsValue(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("like")){
                if(lValue.values().stream().filter(s->s.toString().contains(uValue)).count()>0) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

}
