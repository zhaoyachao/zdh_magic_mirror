package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.PluginInfo;
import com.zyc.magic_mirror.common.plugin.PluginParam;
import com.zyc.magic_mirror.common.plugin.PluginResult;
import com.zyc.magic_mirror.common.plugin.PluginService;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import com.zyc.magic_mirror.plugin.impl.HttpPluginServiceImpl;
import com.zyc.magic_mirror.plugin.impl.KafkaPluginServiceImpl;
import com.zyc.magic_mirror.plugin.impl.PluginServiceImpl;
import com.zyc.magic_mirror.plugin.impl.RedisPluginServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 插件实现
 */
public class PluginCalculateImpl extends BaseCalculate {
    private static Logger logger= LoggerFactory.getLogger(PluginCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1086945890049986561,
     "strategy_context" : "(and)kafka",
     "group_id" : "1086945820982382592",
     "group_context" : "测试kafka插件",
     "group_instance_id" : "1086945890016432128",
     "instance_type" : "plugin",
     "start_time" : "2023-03-19 09:34:53",
     "end_time" : "2023-03-19 09:33:30",
     "jsmind_data" : "{\"rule_expression_cn\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"kafka\",\"positionX\":264,\"rule_id\":\"kafka\",\"positionY\":358,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)kafka\",\"more_task\":\"plugin\",\"id\":\"1086945690535333888\",\"divId\":\"1086945690535333888\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2023-03-19 09:34:55",
     "update_time" : "2023-03-19 09:34:55",
     "expr" : "",
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : "",
     "schedule_source" : "2",
     "cur_time" : "2023-03-19 09:34:53",
     "run_time" : "2023-03-19 09:34:55",
     "run_jsmind_data" : "{\"rule_expression_cn\":\"kafka\",\"rule_param\":\"[{\\\"param_code\\\":\\\"zk_url\\\",\\\"param_context\\\":\\\"zookeeper链接\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"127.0.0.1:2181\\\",\\\"param_type\\\":\\\"string\\\"},{\\\"param_code\\\":\\\"version\\\",\\\"param_context\\\":\\\"版本\\\",\\\"param_operate\\\":\\\"=\\\",\\\"param_value\\\":\\\"1.0\\\",\\\"param_type\\\":\\\"string\\\"}]\",\"type\":\"plugin\",\"is_disenable\":\"false\",\"time_out\":\"86400\",\"rule_context\":\"kafka\",\"positionX\":264,\"rule_id\":\"kafka\",\"positionY\":358,\"is_base\":\"false\",\"operate\":\"and\",\"touch_type\":\"database\",\"name\":\"(and)kafka\",\"more_task\":\"plugin\",\"id\":\"1086945690535333888\",\"divId\":\"1086945690535333888\"}",
     "next_tasks" : "",
     "pre_tasks" : "1086945890049986560",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "1086945690535333888"
     }
     ]}
     */
    public PluginCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        PluginServiceImpl pluginService=new PluginServiceImpl();
        String logStr="";
        try{

            //获取plugin code
            String rule_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            Gson gson=new Gson();
            List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());


            Map<String,Object> params = getJinJavaCommonParam();
            params.put("rule_params", rule_params);

            params.putAll(getJinJavaParam(strategyLogInfo.getCur_time()));

            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();

            Set<DataPipe> rs3 = Sets.newHashSet();
            Set<DataPipe> rs_error = Sets.newHashSet();
            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                PluginInfo pluginInfo = pluginService.selectById(rule_id);
                PluginService pluginServiceImpl = getPluginService(rule_id);
                //读取已经执行过的信息
                List<DataPipe> his = readHisotryFile(file_dir, rule_id+"_"+strategyLogInfo.getStrategy_instance_id(), Const.FILE_STATUS_ALL);
                Set<String> his2 = his.stream().map(str->str.getUdata()).collect(Collectors.toSet());
                Set<String> hisSet = Sets.newHashSet(his2);

                //获取批量处理还是单个处理?
                String is_batch=run_jsmind_data.getOrDefault("is_batch", "false").toString();

                Set<DataPipe> diff = rs.parallelStream().filter(s->!hisSet.contains(s.getUdata())).collect(Collectors.toSet());
                PluginParam pluginParam = pluginServiceImpl.getPluginParam(rule_params);
                if(is_batch.equalsIgnoreCase("false")){
                    for (DataPipe r: diff){
                        params.putAll(JsonUtil.toJavaMap(r.getExt()));//追加参数
                        PluginResult result = pluginServiceImpl.execute(pluginInfo, pluginParam, r, params);
                        String status=Const.FILE_STATUS_FAIL;
                        if(result.getCode() == 0){
                            rs3.add(r);
                        }else{
                            r.setStatus(status);
                            r.setStatus_desc("plugin error");
                            rs_error.add(r);
                        }
                    }
                }else{
                    PluginResult result = pluginServiceImpl.execute(pluginInfo, pluginParam, diff, params);
                    if(result.getBatchResult() != null){
                        for(DataPipe r: result.getBatchResult()){
                            if(r.getStatus() == Const.FILE_STATUS_SUCCESS){
                                rs3.add(r);
                            }else{
                                rs_error.add(r);
                            }
                        }
                    }else{
                        throw new Exception("插件批量处理返回结果必须是Set<DataPipe>类型");
                    }
                }


                his.parallelStream().forEach(s->{
                    if(s.getStatus().equalsIgnoreCase(Const.FILE_STATUS_SUCCESS)){
                        rs3.add(s);
                    }else{
                        rs_error.add(s);
                    }
                });
                writeFile(file_dir+"/"+rule_id+"_"+strategyLogInfo.getStrategy_instance_id(), rs3);
            }

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs3, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs3, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin plugin run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }


    public PluginService getPluginService(String plugin_code){
        if(plugin_code.equalsIgnoreCase("kafka")){
            return new KafkaPluginServiceImpl();
        }else if(plugin_code.equalsIgnoreCase("http")){
            return new HttpPluginServiceImpl();
        }else if(plugin_code.equalsIgnoreCase("redis")){
            return new RedisPluginServiceImpl();
        }
        return null;
    }
}
