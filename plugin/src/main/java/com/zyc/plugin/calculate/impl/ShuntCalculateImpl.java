package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.ShuntCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 分流实现
 */
public class ShuntCalculateImpl extends BaseCalculate implements ShuntCalculate {
    private static Logger logger= LoggerFactory.getLogger(ShuntCalculateImpl.class);

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

    public ShuntCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String shunt_param_str=run_jsmind_data.getOrDefault("shunt_param","").toString();
            List<Map> shunt_params = JSON.parseArray(shunt_param_str, Map.class);
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            if(shunt_params.size()!=1){
                throw new Exception("分流目前只支持一个分流配置");
            }

            Map shunt_param = shunt_params.get(0);



            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();


            String shunt_type = shunt_param.getOrDefault("shunt_type","num").toString();

            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                if(shunt_type.equalsIgnoreCase("num")){
                    //按量级分流
                    int shunt_value = Integer.parseInt(shunt_param.getOrDefault("shunt_value","0").toString());
                    if(rs.size()<shunt_value){
                        shunt_value = rs.size();
                    }
                    logStr = StrUtil.format("task: {}, shunt_type: num, size: {}, num: {}", strategyLogInfo.getStrategy_instance_id(), rs.size(), shunt_value);
                    LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    rs = rs.stream().limit(shunt_value).collect(Collectors.toSet());
                }else if(shunt_type.equalsIgnoreCase("rate")){
                    //按比例分流
                    Set<String> tmp=Sets.newHashSet() ;
                    String[] shunt_values = shunt_param.getOrDefault("shunt_value","1;100").toString().split(";");
                    String shunt_code = shunt_param.getOrDefault("shunt_code","").toString();
                    if(shunt_values.length != 2){
                        throw new Exception("分流参数不正确,100%分流格式: 1;100 ");
                    }
                    int start = Integer.parseInt(shunt_values[0]);
                    int end = Integer.parseInt(shunt_values[1]);
                    List<String> l=new ArrayList<>(rs);
                    for(int i=0;i<rs.size();i++){
                        int mod = (i%100)+1;
                        if(mod>=start && mod <= end){
                            tmp.add(l.get(i));
                        }
                    }
                    //按任务组实例id+分流器code, 注册index范围,此处需要校验index是否重复
                    //addIndex(group_instance_id+"_"+shunt_code, index, index2);
                    logStr = StrUtil.format("task: {}, shunt_type: rate, size: {}, start: {}, end: {}", strategyLogInfo.getStrategy_instance_id(), rs.size(), start, end);
                    LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    rs = tmp;
                }else if(shunt_type.equalsIgnoreCase("hash")){
                    //按hash一致性分流
                    Set<String> tmp=Sets.newHashSet() ;
                    String[] shunt_values = shunt_param.getOrDefault("shunt_value","1;100").toString().split(";");
                    if(shunt_values.length != 2){
                        throw new Exception("分流参数不正确,100%分流格式: 1;100 ");
                    }
                    int start = Integer.parseInt(shunt_values[0]);
                    int end = Integer.parseInt(shunt_values[1]);
                    List<String> l=new ArrayList<>(rs);
                    for(int i=0;i<rs.size();i++){
                        int mod=Hashing.consistentHash(l.hashCode(),100);
                        if(mod>=start && mod <= end){
                            tmp.add(l.get(i));
                        }
                    }
                    logStr = StrUtil.format("task: {}, shunt_type: rate, size: {}, start: {}, end: {}", strategyLogInfo.getStrategy_instance_id(), rs.size(), start, end);
                    LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    rs = tmp;
                }
            }

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
        }
    }

}
