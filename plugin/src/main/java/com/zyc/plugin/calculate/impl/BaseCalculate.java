package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.Const;
import com.zyc.common.util.FileUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateCommomParam;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseCalculate {

    public static ThreadLocal<String> localVar = new ThreadLocal<>();
    public static ConcurrentHashMap<String,BlockingQueue> queue = new ConcurrentHashMap<String, BlockingQueue>();

    public abstract String getOperate(Map run_jsmind_data);

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

    public String getFileDir(String base_path,String group_id, String group_instance_id){
        String file_dir= String.format("%s/%s/%s", base_path,group_id,group_instance_id);
        return file_dir;
    }

    public String getFilePath(String base_path,String group_id, String group_instance_id, String task_id){
        String file_path= String.format("%s/%s", getFileDir(base_path, group_id, group_instance_id),task_id);
        return file_path;
    }

    public String getFilePath(String file_dir, String task_id){
        String file_path= String.format("%s/%s", file_dir,task_id);
        return file_path;
    }

    /**
     * 解析任务实例,生成计算数据的依赖参数 CalculateCommomParam
     * @param param
     * @param strategyInstanceService
     * @return
     */
    public CalculateCommomParam resovlePreTask(Map param,StrategyInstanceServiceImpl strategyInstanceService){
        String pre_tasks = param.get("pre_tasks").toString();
        List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
        String group_id=param.get("group_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        CalculateCommomParam calculateCommomParam=new CalculateCommomParam.Builder()
                .pre_tasks(pre_tasks)
                .strategyInstanceList(strategyInstances)
                .group_id(group_id)
                .group_instance_id(group_instance_id)
                .build();
        return calculateCommomParam;

    }

    public void addIndex(String key,int start, int end){
        synchronized (queue){
            BlockingQueue<String> b = queue.getOrDefault(key, new LinkedBlockingQueue<String>());
            //遍历整个queue
            Iterator<String> i = b.iterator();
            while (i.hasNext()){
                String next=i.next();
                String[] indexs = next.split(",",-1);
                if(start>=Integer.parseInt(indexs[0]) && start<=Integer.parseInt(indexs[1])){
                    //重复
                    if((Integer.parseInt(indexs[1])-start)<10){
                        start = Integer.parseInt(indexs[1])+1;
                    }
                }
                if(end>=Integer.parseInt(indexs[0]) && end<=Integer.parseInt(indexs[1])){
                    if((Integer.parseInt(indexs[0])-end)<10){
                        end = Integer.parseInt(indexs[0])-1;
                    }
                }
            }
            b.add(start+","+end);
        }

    }

    /**
     * 通用计算流程,解析上游任务,生成当前任务依赖的数据
     * @param base_path
     * @param run_jsmind_data
     * @param param
     * @param strategyInstanceService
     * @return
     * @throws IOException
     */
    public CalculateResult calculateResult(String base_path, Map run_jsmind_data, Map param, StrategyInstanceServiceImpl strategyInstanceService) throws IOException {
        //生成参数
        Set<String> rs=Sets.newHashSet() ;
        //检查标签上游
        CalculateCommomParam calculateCommomParam = resovlePreTask(param, strategyInstanceService);
        String operate = getOperate(run_jsmind_data);
        String file_dir= getFileDir(base_path,calculateCommomParam.getGroup_id(),calculateCommomParam.getGroup_instance_id());
        rs = calculatePreTasks(calculateCommomParam.getPre_tasks(), file_dir, operate,calculateCommomParam.getStrategyInstanceList());

        CalculateResult calculateResult=new CalculateResult.Builder()
                .calculateCommomParam(calculateCommomParam)
                .rs(rs)
                .file_dir(file_dir)
                .build();
        return calculateResult;
    }

    /**
     * 解析上游任务,计算上游统计结果
     * @param pre_tasks
     * @param file_dir
     * @param operator
     * @return
     * @throws IOException
     */
    public Set<String> calculatePreTasks(String pre_tasks, String file_dir, String operator, List<StrategyInstance> strategyInstances) throws IOException {
        Set<String> rs=Sets.newHashSet() ;
        if(!StringUtils.isEmpty(pre_tasks)){
            rs = calculate(Lists.newArrayList(pre_tasks.split(",")), file_dir, operator, strategyInstances);
        }else{
            //如果上游为空,则过滤无效 todo
        }
        return rs;
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
            e.printStackTrace();
        }
        return "";
    }

    public List<String> readFile(String file_path) throws IOException {
        File f=new File(file_path);
        if(f.exists() && f.isFile()){
            return FileUtil.readStringSplit(f, Charset.forName("utf-8"));
        }
        return new ArrayList<>();
    }

    public void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }

    /**
     * plugin运算使用
     * @param pre_tasks
     * @param file_dir
     * @param operate
     * @return
     * @throws IOException
     */
    public Set<String> calculate(List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances) throws IOException {

        if(operate.equalsIgnoreCase("not_use")){
            return Sets.newHashSet();
        }

        Set<String> result = null;
        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
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
                    //计算排除
                    result = Sets.difference(result, set);
                }else if(operate.equalsIgnoreCase("not_use")){
                    return Sets.newHashSet();
                }
            }
        }
        if(result == null){
            result = Sets.newHashSet();
        }
        return result;
    }

    /**
     * 统一写入文件并打印日志
     * @param id
     * @param strategy_id
     * @param file_path
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLog(String id,String strategy_id, String file_path, Set<String> rs) throws IOException {
        String save_path = writeFile(id,file_path, rs);
        String logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
        LogUtil.info(strategy_id, id, logStr);
        setStatus(id, Const.STATUS_FINISH);
        logStr = StrUtil.format("task: {}, update status finish", id);
        LogUtil.info(strategy_id, id, logStr);
    }
}
