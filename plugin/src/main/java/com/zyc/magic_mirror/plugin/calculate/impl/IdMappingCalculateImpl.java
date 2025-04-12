package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import com.zyc.magic_mirror.plugin.calculate.IdMappingEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * id_mapping实现
 */
public class IdMappingCalculateImpl extends BaseCalculate {
    private static Logger logger= LoggerFactory.getLogger(IdMappingCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1036227672688037890,
     "strategy_context" : "cessss",
     "group_id" : "1033156620940480512",
     "group_context" : "测试第一个标签策略",
     "group_instance_id" : "1036227672352493568",
     "instance_type" : "shunt",
     "start_time" : "2022-10-30 10:38:47",
     "end_time" : null,
     "jsmind_data" : "{\"positionY\":461,\"shunt_context\":\"cessss\",\"shunt\":\"9bd_140_a7bc_ea\",\"touch_type\":\"database\",\"shunt_param\":\"[{\\\"shunt_code\\\":\\\"A\\\",\\\"shunt_name\\\":\\\"分流3个\\\",\\\"shunt_type\\\":\\\"in\\\",\\\"shunt_value\\\":\\\"3\\\"}]\",\"name\":\"cessss\",\"more_task\":\"shunt\",\"id\":\"c42_8c8_8b1a_2a\",\"type\":\"shunt\",\"divId\":\"c42_8c8_8b1a_2a\",\"time_out\":\"86400\",\"positionX\":311}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2022-10-21 23:15:34",
     "update_time" : "2022-10-30 10:38:49",
     "expr" : null,
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : null,
     "schedule_source" : "2",
     "cur_time" : "2022-10-30 10:38:47",
     "run_time" : "2022-10-30 10:38:49",
     "run_jsmind_data" : "{\"positionY\":461,\"shunt_context\":\"cessss\",\"shunt\":\"9bd_140_a7bc_ea\",\"touch_type\":\"database\",\"shunt_param\":\"[{\\\"shunt_code\\\":\\\"A\\\",\\\"shunt_name\\\":\\\"分流3个\\\",\\\"shunt_type\\\":\\\"in\\\",\\\"shunt_value\\\":\\\"3\\\"}]\",\"name\":\"cessss\",\"more_task\":\"shunt\",\"id\":\"c42_8c8_8b1a_2a\",\"type\":\"shunt\",\"divId\":\"c42_8c8_8b1a_2a\",\"time_out\":\"86400\",\"positionX\":311}",
     "next_tasks" : "",
     "pre_tasks" : "1036227672688037889",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "c42_8c8_8b1a_2a"
     }
     ]}
     */
    public IdMappingCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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

            //获取标签code
            Map run_jsmind_data = JsonUtil.toJavaBean(this.param.get("run_jsmind_data").toString(), Map.class);
            String id_mapping_code=run_jsmind_data.get("rule_id").toString();
            String id_mapping_type=run_jsmind_data.getOrDefault("id_mapping_type","").toString();
            String data_engine=run_jsmind_data.getOrDefault("data_engine", "file").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            logger.info("task: {}, merge upstream data start", strategyLogInfo.getStrategy_instance_id());
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();


            logger.info("task: {}, merge upstream data end, size: {}", strategyLogInfo.getStrategy_instance_id(), rs.size());

            Set<DataPipe> rs_error=Sets.newHashSet() ;//映射结果,映射成功的结果

            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                //读取id_mapping
                logger.info("task: {}, id mapping start", strategyLogInfo.getStrategy_instance_id());
                // todo 此处需要做调整根据mapping_code找到对应的数据文件
                //选择id mapping 存储引擎
                IdMappingEngine idMappingEngine = readIdMappingData(data_engine,strategyLogInfo.getBase_path(), id_mapping_code);
                IdMappingEngine.IdMappingResult idMappingResult = idMappingEngine.getMap(rs);
                rs = idMappingResult.rs;
                rs_error = idMappingResult.rs_error;
            }

            //映射结果
            String file_idmapping_path = getFilePath(file_dir, "idmapping_"+strategyLogInfo.getStrategy_instance_id());
            String save_idmapping_path = writeFile(file_idmapping_path, rs);

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin idmapping run error: ", e);
        }finally {

        }
    }

    public IdMappingEngine readIdMappingData(String data_engine, String base_path, String id_mapping_code) throws Exception {

        if(data_engine.equalsIgnoreCase("file")){
            String file_path=base_path+"/id_mapping/"+data_engine+"/"+id_mapping_code;
            FileIdMappingEngineImpl idMappingEngine = new FileIdMappingEngineImpl(file_path, id_mapping_code);
            return idMappingEngine;
        }else if(data_engine.equalsIgnoreCase("rocksdb")){
            String file_path=base_path+"/id_mapping/"+data_engine+"/"+id_mapping_code;
            RocksDbIdMappingEngineImpl idMappingEngine = new RocksDbIdMappingEngineImpl(file_path, id_mapping_code);
            return idMappingEngine;
        }else if(data_engine.equalsIgnoreCase("jdbc")){

        }else if(data_engine.equalsIgnoreCase("es")){

        }else if(data_engine.equalsIgnoreCase("redis")){
            //根据id_mapping_code找到对应redis配置
            RedisIdMappingEngineImpl idMappingEngine = new RedisIdMappingEngineImpl(id_mapping_code);
            return idMappingEngine;
        }
        throw new Exception("不支持的id mapping计算引擎");
    }
}
