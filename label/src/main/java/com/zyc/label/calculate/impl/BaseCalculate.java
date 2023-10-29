package com.zyc.label.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.FileUtil;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;

public class BaseCalculate {

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
     *
     * @param pre_tasks
     * @param file_dir
     * @param operate or,and, not
     * @return
     * @throws IOException
     */
    public Set<String> calculatePreTasks(String pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances) throws IOException {
        Set<String> rs=Sets.newHashSet() ;
        if(!StringUtils.isEmpty(pre_tasks)){
            //如果是排除逻辑, 上游多个任务先取并集,然后再排除当前标签,此处默认使用并集
            rs = calculate(Arrays.asList(pre_tasks.split(",")), file_dir, operate, strategyInstances);
        }else{
            //如果上游为空,则过滤无效 todo
        }
        return rs;
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
    public Set<String> calculate(String file_dir, List<String> pre_tasks, String operate, Set<String> cur_rows, List<StrategyInstance> strategyInstances,String is_disenable) throws IOException {

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
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"));
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
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"));
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
                List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"));
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
     * @param pre_tasks
     * @param file_dir
     * @param operate
     * @param strategyInstances
     * @return
     * @throws IOException
     */
    public Set<String> calculate(List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances) throws IOException {
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
                    List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"));
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
            List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"));
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
}
