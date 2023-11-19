package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.IdMappingCalculate;
import com.zyc.plugin.calculate.IdMappingEngine;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * id_mapping实现
 */
public class IdMappingCalculateImpl extends BaseCalculate implements IdMappingCalculate {
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
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public IdMappingCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        //唯一任务ID
        String id=this.param.get("id").toString();
        String group_id=this.param.get("group_id").toString();
        String strategy_id=this.param.get("strategy_id").toString();
        String group_instance_id=this.param.get("group_instance_id").toString();
        String logStr="";
        String file_path=getFilePathByParam(this.param, this.dbConfig);
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String id_mapping_code=run_jsmind_data.get("id_mapping_code").toString();
            String id_mapping_type=run_jsmind_data.getOrDefault("id_mapping_type","").toString();
            String data_engine=run_jsmind_data.getOrDefault("data_engine", "file").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //调度逻辑时间,毫秒时间戳
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }

            String base_path=dbConfig.get("file.path");
            //解析参数,生成人群

            //生成参数
            logger.info("task: {}, merge upstream data start", id);
            CalculateResult calculateResult = calculateResult(base_path, run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();

            file_path = getFilePath(file_dir, id);

            logger.info("task: {}, merge upstream data end, size: {}", id, rs.size());
            Set<String> rs2=Sets.newHashSet() ;//映射明细,每条记录的映射关系
            Set<String> rs3=Sets.newHashSet() ;//映射结果,映射成功的结果
            if(is_disenable.equalsIgnoreCase("true")){
                rs2=rs;
                rs3=rs;
            }else{
                //读取id_mapping
                logger.info("task: {}, id mapping start", id);
                // todo 此处需要做调整根据mapping_code找到对应的数据文件
                //选择id mapping 存储引擎
                IdMappingEngine idMappingEngine = readIdMappingData(data_engine,base_path, id_mapping_code);
                List<String> id_mappings = idMappingEngine.get();
                Map<String,String> id_map = Maps.newHashMap();
                for (String line:id_mappings){
                    String[] idm = line.split(",",2);
                    id_map.put(idm[0], idm[1]);
                }

                Iterator<String> rs1 = rs.iterator();
                while (rs1.hasNext()){
                    String key = rs1.next();
                    if(id_map.containsKey(key)){
                        rs2.add(id_map.get(key)+","+key);
                        rs3.add(id_map.get(key));
                    }
                }
            }

            //映射结果
            String file_idmapping_path = getFilePath(file_dir, "idmapping_"+id);
            String save_idmapping_path = writeFile(id,file_idmapping_path, rs2);
            logStr = StrUtil.format("task: {}, id mapping end, size: {}", id, rs3.size());
            LogUtil.info(strategy_id, id, logStr);

            writeFileAndPrintLog(id,strategy_id, file_path, rs3);

        }catch (Exception e){
            writeEmptyFile(file_path);
            setStatus(id, Const.STATUS_ERROR);
            LogUtil.error(strategy_id, id, e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
        }
    }

    public IdMappingEngine readIdMappingData(String data_engine, String base_path, String id_mapping_code) throws Exception {

        if(data_engine.equalsIgnoreCase("file")){
            String file_path=base_path+"/id_mapping/"+data_engine+"/"+id_mapping_code;
            FileIdMappingEngineImpl idMappingEngine = new FileIdMappingEngineImpl(file_path);
            return idMappingEngine;
        }else if(data_engine.equalsIgnoreCase("rocksdb")){
            String file_path=base_path+"/id_mapping/"+data_engine+"/"+id_mapping_code;
            RocksDbIdMappingEngineImpl idMappingEngine = new RocksDbIdMappingEngineImpl(file_path);
            return idMappingEngine;
        }else if(data_engine.equalsIgnoreCase("jdbc")){

        }else if(data_engine.equalsIgnoreCase("es")){

        }
        throw new Exception("不支持的id mapping计算引擎");
    }
}
