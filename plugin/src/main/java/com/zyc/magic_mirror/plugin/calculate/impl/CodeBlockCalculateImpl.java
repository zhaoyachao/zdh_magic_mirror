package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.groovy.GroovyFactory;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 插件实现
 */
public class CodeBlockCalculateImpl extends BaseCalculate{
    private static Logger logger= LoggerFactory.getLogger(CodeBlockCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1156712125243068416,
     "strategy_context" : "(and)测试java输出json",
     "group_id" : "1000709645808963584",
     "group_context" : "测试策略组",
     "group_instance_id" : "1156712122944589824",
     "instance_type" : "code_block",
     "start_time" : "2023-09-27 22:00:59",
     "end_time" : "2023-01-18 08:00:00",
     "jsmind_data" : "{\"type\":\"code_block\",\"is_disenable\":\"false\",\"command\":\"System.out.println(\\\"123\\\")\",\"time_out\":\"86400\",\"rule_context\":\"测试java输出json\",\"positionX\":334,\"rule_id\":\"1156700497957097472\",\"positionY\":352,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)测试java输出json\",\"more_task\":\"code_block\",\"id\":\"1156700497957097472\",\"divId\":\"1156700497957097472\",\"depend_level\":\"0\",\"code_type\":\"java\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-09-27 22:01:01",
     "update_time" : "2023-09-27 22:01:01",
     "expr" : "0 0 * * * ? *",
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : "on",
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-09-27 22:00:59",
     "run_time" : "2023-09-27 22:01:01",
     "run_jsmind_data" : "{\"type\":\"code_block\",\"is_disenable\":\"false\",\"command\":\"System.out.println(\\\"123\\\")\",\"time_out\":\"86400\",\"rule_context\":\"测试java输出json\",\"positionX\":334,\"rule_id\":\"1156700497957097472\",\"positionY\":352,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)测试java输出json\",\"more_task\":\"code_block\",\"id\":\"1156700497957097472\",\"divId\":\"1156700497957097472\",\"depend_level\":\"0\",\"code_type\":\"java\"}",
     "next_tasks" : "",
     "pre_tasks" : "1156712125205319680",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "1156700497957097472",
     "group_type" : "offline",
     "data_node" : ""
     }
     ]}
     */

    public CodeBlockCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void process() {
        String logStr="";
        try{
            //获取plugin code
            String rule_id=run_jsmind_data.getOrDefault("rule_id", "").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //生成参数
            Gson gson=new Gson();

            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();

            Set<DataPipe> rs_ret = Sets.newHashSet();

            Map<String,Object> params = getJinJavaCommonParam();

            //mergeMapByVarPool(strategyLogInfo.getStrategy_group_instance_id(), params);

            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "代码块输入参数: "+gson.toJson(params));
                String code_type=run_jsmind_data.getOrDefault("code_type", "").toString();
                String command=run_jsmind_data.getOrDefault("command", "").toString();
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), command);
                //入参 上游数据集
                params.put("rs", rs);
                Object result = null;
                if(code_type.equalsIgnoreCase("java")){
                    result = GroovyFactory.execJavaCode(command, params);
                }else if(code_type.equalsIgnoreCase("groovy")){
                    params.put("out", new HashMap<>());
                    result = GroovyFactory.execExpress(command, params);
                }else{
                    throw new Exception("不支持的代码类型,目前仅支持java,groovy");
                }

                if(result instanceof Map){
                    if(((Map) result).containsKey("out_rs")){
                        rs_ret = (Set<DataPipe>)((Map) result).getOrDefault("out_rs", Sets.newHashSet());
                    }else{
                        throw new Exception("java代码返回结果类型仅支持map结构,且返回的map结构中必须包含out_rs结果集");
                    }
                }else{
                    throw new Exception("java代码返回结果类型仅支持map结构");
                }
            }

            Set<String> rs_error = Sets.difference(Sets.newHashSet(rs.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet())), Sets.newHashSet(rs_ret.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet())));

            Set<DataPipe> rowsErrorObj= Sets.newHashSet(rs_error.stream().map(s -> new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_FAIL)
                    .udata_type("").
                            task_type(InstanceType.CODE_BLOCK.getCode()).build()).collect(Collectors.toSet()));

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs_ret, rowsErrorObj);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs_ret, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin codeblock run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }
}
