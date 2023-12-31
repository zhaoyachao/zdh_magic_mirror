package com.zyc.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.FileUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.RocksDBUtil;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;

public class BaseCalculate {


    /**
     * 初始化基础参数
     * @param param
     * @param dbConfig
     * @return
     */
    public StrategyLogInfo init(Map<String,Object> param, Map<String,String> dbConfig){
        String id=param.get("id").toString();
        String group_id=param.get("group_id").toString();
        String strategy_id=param.get("strategy_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        String cur_time=param.get("cur_time").toString();
        String base_path=dbConfig.get("file.path");
        String file_path=getFilePathByParam(param, dbConfig);
        String file_rocksdb_path = getRocksdbFileDirByParam(param, dbConfig);
        StrategyLogInfo strategyLogInfo = new StrategyLogInfo();
        strategyLogInfo.setStrategy_instance_id(id);
        strategyLogInfo.setStrategy_group_instance_id(group_instance_id);
        strategyLogInfo.setStrategy_group_id(group_id);
        strategyLogInfo.setStrategy_id(strategy_id);
        strategyLogInfo.setCur_time(new Timestamp(Long.valueOf(cur_time)));
        strategyLogInfo.setBase_path(base_path);
        strategyLogInfo.setFile_path(file_path);
        strategyLogInfo.setFile_rocksdb_path(file_rocksdb_path);
        return strategyLogInfo;
    }

    /**
     * 根据策略配置和系统配置目录获取文件写入地址
     * @param param
     * @param dbConfig
     * @return
     */
    public String getFilePathByParam(Map param, Map dbConfig){
        String base_path=dbConfig.get("file.path").toString();
        String id=param.get("id").toString();
        String group_id=param.get("group_id").toString();
        String strategy_id=param.get("strategy_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        return getFilePath(base_path,group_id,group_instance_id,id);
    }

    /**
     * 根据策略配置和系统配置目录获取rocksdb文件写入地址
     * @param param
     * @param dbConfig
     * @return
     */
    public String getRocksdbFileDirByParam(Map param, Map dbConfig){
        String base_path=dbConfig.get("file.rocksdb.path").toString();
        String group_id=param.get("group_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        return getFileDir(base_path,group_id,group_instance_id);
    }

    /**
     * 获取任务写入目录
     * @param base_path
     * @param group_id
     * @param group_instance_id
     * @return
     */
    public String getFileDir(String base_path,String group_id, String group_instance_id){
        String file_dir= String.format("%s/%s/%s", base_path,group_id,group_instance_id);
        return file_dir;
    }

    /**
     * 获取任务写入文件全路径
     * @param base_path
     * @param group_id
     * @param group_instance_id
     * @param task_id
     * @return
     */
    public String getFilePath(String base_path,String group_id, String group_instance_id, String task_id){
        String file_path= String.format("%s/%s", getFileDir(base_path, group_id, group_instance_id),task_id);
        return file_path;
    }

    public String getFilePath(String file_dir, String task_id){
        String file_path= String.format("%s/%s", file_dir,task_id);
        return file_path;
    }

    /**
     * 返回文件绝对路径
     * @param task_id
     * @param file_path
     * @param rows
     * @return
     * @throws IOException
     */
    public String writeFile(String task_id, String file_path, Set<String> rows) throws IOException {
        File f=new File(file_path);
        if(!new File(f.getParent()).exists()){
            new File(f.getParent()).mkdirs();
        }
        BufferedWriter bw = FileUtil.createBufferedWriter(f, Charset.forName("utf-8"));
        for (String line:rows){
            FileUtil.writeString(bw, line);
        }
        FileUtil.flush(bw);
        return f.getAbsolutePath();
    }

    public void writeEmptyFileAndStatus(StrategyLogInfo strategyLogInfo){
        writeEmptyFile(strategyLogInfo.getFile_path());
        setStatus(strategyLogInfo.getStrategy_instance_id(), Const.STATUS_ERROR);
    }

    public String writeEmptyFile(String file_path){
        try{
            File f=new File(file_path);
            if(!new File(f.getParent()).exists()){
                new File(f.getParent()).mkdirs();
            }
            BufferedWriter bw = FileUtil.createBufferedWriter(f, Charset.forName("utf-8"));
            FileUtil.flush(bw);
            return f.getAbsolutePath();
        }catch (Exception e){

        }
        return "";
    }

    public void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(new Date().getTime()));
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }


    /**
     * 解析上游任务和当前节点任务做运算-入口
     * @param currentRows
     * @param is_disenable
     * @param file_dir
     * @param param
     * @param run_jsmind_data
     * @param strategyInstanceService
     * @return
     * @throws IOException
     */
    public Set<String> calculateCommon(Set<String> currentRows,String is_disenable, String file_dir, Map param,Map run_jsmind_data, StrategyInstanceServiceImpl strategyInstanceService) throws IOException {
        String pre_tasks = param.get("pre_tasks").toString();
        List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
        String operate=run_jsmind_data.get("operate").toString();
        String status=run_jsmind_data.getOrDefault("data_status",Const.FILE_STATUS_SUCCESS).toString();//依赖数据状态,1:成功,2:失败,3:不区分
        List<String> pre_tasks_list = Lists.newArrayList();
        if(!StringUtils.isEmpty(pre_tasks)){
            pre_tasks_list = Lists.newArrayList(pre_tasks.split(","));
        }
        return calculate(file_dir, pre_tasks_list, operate, currentRows, strategyInstances, is_disenable, status);
    }

    /**
     * 标签/人群文件使用
     * 操作符not_use,表示不使用上游数据
     * 如果策略被禁用或者跳过,那么当前策略的数据直接使用上游数据
     * @param pre_tasks
     * @param operate
     * @param cur_rows
     * @param strategyInstances
     * @param is_disenable 是否禁用，true:禁用,false:未禁用
     * @return
     * @throws IOException
     */
    public Set<String> calculate(String file_dir, List<String> pre_tasks, String operate, Set<String> cur_rows, List<StrategyInstance> strategyInstances,
                                 String is_disenable, String status) throws IOException {

        //无上游,则直接返回当前结果集
        if(pre_tasks==null || pre_tasks.size()== 0){
            return cur_rows;
        }

        //指定不使用上游数据,返回当前结果集
        if(operate.equalsIgnoreCase("not_use")){
            return cur_rows;
        }

        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
        }

        Set<String> result=Sets.newHashSet();


        //如果是排除逻辑, 上游多个任务先取并集,然后再排除当前标签
        if(operate.equalsIgnoreCase("not")){
            //取所有上游的并集
            for(String task:pre_tasks){
                if(map.get(task).getStatus().equalsIgnoreCase("skip")){
                    //skip 任务逻辑修改,跳过/禁用任务,采用上游数据
                   //continue;
                }
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                Set<String> set = Sets.newHashSet(lines);
                result = Sets.difference(result, set);
            }
            if(is_disenable.equalsIgnoreCase("false")){
                result = Sets.intersection(result, cur_rows);
            }
        }else if(operate.equalsIgnoreCase("and")){
            //交集
            result = null;
            for(String task:pre_tasks){
                if(map.get(task).getStatus().equalsIgnoreCase("skip")){
                   //continue;
                }
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                Set<String> set = Sets.newHashSet(lines);
                if(result == null){
                    result = set;
                }else{
                    result = Sets.intersection(result, set);
                }
            }
            if(result == null){
                result = Sets.newHashSet();
            }
            if(is_disenable.equalsIgnoreCase("false")){
                result = Sets.intersection(result, cur_rows);
            }
        }else if(operate.equalsIgnoreCase("or")){
            //取并集去重
            result = null;
            for(String task:pre_tasks){
                if(map.get(task).getStatus().equalsIgnoreCase("skip")){
                    //continue;
                }
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                Set<String> set = Sets.newHashSet(lines);
                if(result == null){
                    result = set;
                }else{
                    result = Sets.union(result, set);
                }
            }
            if(result == null){
                result = Sets.newHashSet();
            }
            if(is_disenable.equalsIgnoreCase("false")){
                result = Sets.intersection(result, cur_rows);
            }
        }else if(operate.equalsIgnoreCase("not_use")){
            if(result == null){
                result = Sets.newHashSet();
            }
            if(is_disenable.equalsIgnoreCase("false")){
                return cur_rows;
            }
        }

        return result;
    }

    /**
     * 人群运算符使用
     * 操作符not_use,表示不使用上游数据
     * 使用not排除逻辑时,需要指定一个base数据,用于做排除的基准数据
     * @param pre_tasks
     * @param file_dir
     * @param operate
     * @param strategyInstances
     * @return
     * @throws IOException
     */
    public Set<String> calculate(List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances, String status) throws IOException {
        if(operate.equalsIgnoreCase("not_use")){
            return Sets.newHashSet();
        }

        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
        }

        Set<String> result=Sets.newHashSet();
        //如果是排除逻辑,需要先找一个base数据基于这个数据做排除
        if(operate.equalsIgnoreCase("not")){
            //需要先找到一个base
            result = Sets.newHashSet();
            for(String task:pre_tasks){
                Map run_jsmind_data = JSON.parseObject(map.get(task).getRun_jsmind_data(), Map.class);
                if(run_jsmind_data.getOrDefault("is_base","false").equals("true")){
                    List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                    result =Sets.newHashSet(rows);
                    pre_tasks.remove(task);
                    break ;
                }
            }
        }

        //多个任务交并排逻辑
        for(String task:pre_tasks){
            if(map.get(task).getStatus().equalsIgnoreCase("skip")){
                //continue;
            }
            List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
            Set<String> set=Sets.newHashSet(rows);
            if(result==null){
                //第一次赋值
                result = set;
            }else{
                if(operate.equalsIgnoreCase("or")){
                    //计算并集
                    result = Sets.union(result, set);
                }else if(operate.equalsIgnoreCase("and")){
                    //计算交集
                    result = Sets.intersection(result, set);
                }else if(operate.equalsIgnoreCase("not")){
                    result = Sets.difference(result, set);
                }else if(operate.equalsIgnoreCase("not_use")){
                    return Sets.newHashSet();
                }
            }
        }
        return result;
    }

    /**
     * 统一写入文件并打印日志
     * @param strategyLogInfo
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLogAndUpdateStatus2Finish(StrategyLogInfo strategyLogInfo,  Set<String> rs) throws IOException {
        String logStr = StrUtil.format("task: {}, calculate finish size: {}", strategyLogInfo.getStrategy_instance_id(), rs.size());
        LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
        writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo.getStrategy_instance_id(),strategyLogInfo.getStrategy_id(), strategyLogInfo.getFile_path(), rs);
    }

    /**
     * 统一写入文件并打印日志
     * @param id
     * @param strategy_id
     * @param file_path
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLogAndUpdateStatus2Finish(String id,String strategy_id, String file_path, Set<String> rs) throws IOException {
        String save_path = writeFile(id,file_path, rs);
        String logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
        LogUtil.info(strategy_id, id, logStr);
        setStatus(id, Const.STATUS_FINISH);
        logStr = StrUtil.format("task: {}, update status finish", id);
        LogUtil.info(strategy_id, id, logStr);
    }

    /**
     * 结果写入rocksdb
     * @param file_rocksdb_path
     * @param id
     * @param rs
     * @param status
     * @throws Exception
     */
    public void writeRocksdb(String file_rocksdb_path, String id, Set<String> rs, String status) throws Exception {

        File file = new File(file_rocksdb_path);
        if(!file.exists()){
            file.mkdirs();
        }
        RocksDB rocksDB = RocksDBUtil.getConnection(file_rocksdb_path);
        if(rs != null && rs.size() > 0){
            for (String r: rs){
                String key = r+"_"+id;
                rocksDB.put(key.getBytes(), status.getBytes());
            }
        }
        rocksDB.close();
    }
}
