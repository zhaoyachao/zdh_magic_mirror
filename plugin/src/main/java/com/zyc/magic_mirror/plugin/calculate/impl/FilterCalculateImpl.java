package com.zyc.magic_mirror.plugin.calculate.impl;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.FilterInfo;
import com.zyc.magic_mirror.common.entity.InstanceType;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.FileUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import com.zyc.magic_mirror.plugin.calculate.FilterEngine;
import com.zyc.magic_mirror.plugin.impl.FilterServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 过滤实现
 */
public class FilterCalculateImpl extends BaseCalculate{
    private static Logger logger= LoggerFactory.getLogger(FilterCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1036063427954479104,
     "strategy_context" : "过滤规则",
     "group_id" : "1033156620940480512",
     "group_context" : "测试第一个标签策略",
     "group_instance_id" : "1036063427644100608",
     "instance_type" : "filter",
     "start_time" : "2022-10-29 23:46:08",
     "end_time" : null,
     "jsmind_data" : "{\"rule_context\":\"过滤规则\",\"type\":\"filter\",\"time_out\":\"86400\",\"positionX\":364,\"filter\":\"age18,test_filter_code1\",\"positionY\":278,\"filter_title\":\"年龄大于18岁,测试过滤规则111\",\"touch_type\":\"database\",\"name\":\"过滤规则\",\"more_task\":\"filter\",\"id\":\"922_a6f_8224_17\",\"divId\":\"922_a6f_8224_17\",\"depend_level\":\"0\"}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2022-10-21 23:15:34",
     "update_time" : "2022-10-29 23:46:10",
     "expr" : null,
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : null,
     "schedule_source" : "2",
     "cur_time" : "2022-10-29 23:46:08",
     "run_time" : "2022-10-29 23:46:10",
     "run_jsmind_data" : "{\"rule_context\":\"过滤规则\",\"type\":\"filter\",\"time_out\":\"86400\",\"positionX\":364,\"filter\":\"age18,test_filter_code1\",\"positionY\":278,\"filter_title\":\"年龄大于18岁,测试过滤规则111\",\"touch_type\":\"database\",\"name\":\"过滤规则\",\"more_task\":\"filter\",\"id\":\"922_a6f_8224_17\",\"divId\":\"922_a6f_8224_17\",\"depend_level\":\"0\"}",
     "next_tasks" : "1036063427958673408",
     "pre_tasks" : "1036063427946090496",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "922_a6f_8224_17"
     }
     ]}

     */

    public FilterCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        FilterServiceImpl filterService=new FilterServiceImpl();
        String logStr="";
        try{
            //获取标签code
            String[] filter_codes=run_jsmind_data.getOrDefault("rule_id","").toString().split(",");
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            List<FilterInfo> filterInfos=new ArrayList<>();

            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();
            Set<String> rs_error = Sets.newHashSet();
            if(is_disenable.equalsIgnoreCase("true")){

            }else{
                for (String filter_code: filter_codes){
                    FilterInfo filterInfo = filterService.selectByCode(filter_code);
                    if(filterInfo==null){
                        throw new Exception("无法找到过滤规则信息");
                    }
                    filterInfos.add(filterInfo);
                }
                LogUtil.warn(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), "过滤规则使用redis引擎时,需要区分产品线,请谨慎使用!!!");
                //执行过滤
                for (FilterInfo filterInfo:filterInfos){
                    filterInfo.getFilter_name();
                    FilterEngine filterEngine = getFilterEngine(filterInfo, strategyLogInfo.getBase_path());
                    FilterEngine.FilterResult filterResult = filterEngine.getMap(rs);
                    rs = Sets.newHashSet(filterResult.getRs());
                    rs_error = Sets.union(rs_error, filterResult.getRs_error().parallelStream().map(s->s.getUdata()).collect(Collectors.toSet()));
                }
            }


            Set<DataPipe> rowsErrorObj= Sets.newHashSet(rs_error.stream().map(s -> new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_FAIL)
                    .udata_type("").
                            task_type(InstanceType.FILTER.getCode()).build()).collect(Collectors.toSet()));

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rowsErrorObj);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin filter run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }


    private Set<String> loadFilters(FilterInfo filterInfo,String base_path) throws Exception {
        if(filterInfo.getEngine_type().equalsIgnoreCase("file")){
            String filter_path=base_path+"/filter/"+filterInfo.getFilter_code();
            String split = "\t";
            List<String> list = FileUtil.readTextFirstSplit(new File(filter_path), Charset.forName("utf-8"), split);
            Set<String> filterDataFrame = Sets.newHashSet();
            filterDataFrame.addAll(list);
            return filterDataFrame;
        }else{
            throw new Exception("暂不支持的计算引擎");
        }
    }

    private FilterEngine getFilterEngine(FilterInfo filterInfo,String base_path) throws Exception {
        if(filterInfo.getEngine_type().equalsIgnoreCase("file")){
            String filter_path=base_path+"/filter/"+filterInfo.getFilter_code();
            FilterEngine filterEngine = new FileFilterEngineImpl(filterInfo, filter_path);
            return filterEngine;
        }else  if(filterInfo.getEngine_type().equalsIgnoreCase("redis")){
            RedisFilterEngineImpl filterEngine = new RedisFilterEngineImpl(filterInfo.getFilter_code(), filterInfo.getProduct_code());
            return filterEngine;
        }else{
            throw new Exception("暂不支持的计算引擎");
        }
    }

}
