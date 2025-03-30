package com.zyc.label.calculate.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcraft.jsch.SftpException;
import com.zyc.common.entity.DataPipe;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.*;
import com.zyc.label.LabelServer;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import io.minio.MinioClient;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@SuppressWarnings("ALL")
public class BaseCalculate {

    private static Logger logger= LoggerFactory.getLogger(BaseCalculate.class);

    private SFTPUtil sftpUtil;

    private MinioClient minioClient;

    private Map<String, Object> jinJavaCommonParam = new HashMap<>();

    private String split = "\t";

    public SFTPUtil getSftpUtil(Map<String,String> dbConfig){
        if(!dbConfig.getOrDefault("sftp.enable", "false").equalsIgnoreCase("true")){
            return sftpUtil;
        }

        String username=dbConfig.get("sftp.username");
        String password=dbConfig.get("sftp.password");
        String host=dbConfig.get("sftp.host");
        int port=Integer.parseInt(dbConfig.get("sftp.port"));
        sftpUtil=new SFTPUtil(username, password, host, port);
        return sftpUtil;
    }

    public void initMinioClient(Map<String,String> dbConfig){
        if(!dbConfig.getOrDefault("storage.mode", "").equalsIgnoreCase("minio")){
            return ;
        }
        String ak = dbConfig.get("storage.minio.ak");
        String sk=dbConfig.get("storage.minio.sk");
        String endpoint=dbConfig.get("storage.minio.endpoint");
        minioClient = MinioUtil.buildMinioClient(ak, sk, endpoint);
    }

    /**
     * 检查是否开启sftp,默认不开启,如果需要开启需要重写子类checkSftp
     * @return
     */
    public boolean checkSftp(){
        return false;
    }

    /**
     * 返回存储模式
     * 当前可选择值为minio
     * @return
     */
    public String storageMode(){
        return "";
    }

    /**
     * 获取通用bucket
     * @return
     */
    public String getBucket(){
        return "zdh-magic-mirror";
    }

    /**
     * 获取通用region
     * @return
     */
    public String getRegion(){
        return "cn-north-1";
    }

    public MinioClient getMinioClient(){
        return minioClient;
    }

    /**
     * 初始化公共参数
     * @param strategyLogInfo
     * @param param
     */
    public void initJinJavaCommonParam(StrategyLogInfo strategyLogInfo, Map<String, Object> param){
        Map<String, Object> systemParam = getJinJavaParam(strategyLogInfo.getCur_time());

        systemParam.put("stragegy_instance", param);
        systemParam.put("stragegy_instance_id", param.get("id"));

        systemParam.put("cur_time", cn.hutool.core.date.DateUtil.format(strategyLogInfo.getCur_time(), DatePattern.NORM_DATETIME_PATTERN));

        this.jinJavaCommonParam = systemParam;
    }

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
        //todo fastjson timestamp类型自动转换成long类型
        String cur_time=param.get("cur_time").toString();
        String cur_date = DateUtil.format(Timestamp.valueOf(cur_time));

        String base_path=getBasePathWithCurDate(dbConfig.get("file.path"), cur_date);
        String base_rocksdb_path=getBasePathWithCurDate(dbConfig.get("file.rocksdb.path").toString(), cur_date);

        String file_path=getFilePathByParam(base_path, param, dbConfig);
        String file_rocksdb_path = getRocksdbFileDirByParam(base_rocksdb_path, param, dbConfig);

        StrategyLogInfo strategyLogInfo = new StrategyLogInfo();
        strategyLogInfo.setStrategy_instance_id(id);
        strategyLogInfo.setStrategy_group_instance_id(group_instance_id);
        strategyLogInfo.setStrategy_group_id(group_id);
        strategyLogInfo.setStrategy_id(strategy_id);
        strategyLogInfo.setInstance_type(param.get("instance_type").toString());
        //strategyLogInfo.setCur_time(new Timestamp(Long.valueOf(cur_time)));
        strategyLogInfo.setCur_time(Timestamp.valueOf(cur_time));
        strategyLogInfo.setBase_path(base_path);
        strategyLogInfo.setFile_path(file_path);
        strategyLogInfo.setFile_rocksdb_path(file_rocksdb_path);
        return strategyLogInfo;
    }

    /**
     * 根据逻辑运行日期-生成新路径
     * @param path
     * @param cur_date
     * @return
     */
    public String getBasePathWithCurDate(String path, String cur_date){
        if(path.endsWith("/")){
            return path+cur_date;
        }
        return path + "/" + cur_date;
    }

    /**
     * 根据策略配置和系统配置目录获取文件写入地址
     * @param base_path
     * @param param
     * @param dbConfig
     * @return
     */
    public String getFilePathByParam(String base_path,Map param, Map dbConfig){
        String id=param.get("id").toString();
        String group_id=param.get("group_id").toString();
        String strategy_id=param.get("strategy_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        return getFilePath(base_path,group_id,group_instance_id,id);
    }

    /**
     * 根据策略配置和系统配置目录获取rocksdb文件写入地址
     * @param base_rocksdb_path
     * @param param
     * @param dbConfig
     * @return
     */
    public String getRocksdbFileDirByParam(String base_rocksdb_path, Map param, Map dbConfig){
        String group_id=param.get("group_id").toString();
        String group_instance_id=param.get("group_instance_id").toString();
        String id=param.get("id").toString();
        return getFilePath(getFileDir(base_rocksdb_path, group_id,group_instance_id), id);
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
     *
     * 写入文件,会自动追加状态,因此需要函数使用方保证数据的正确性,状态都是在第2位
     * @param file_path
     * @param rows
     * @return
     * @throws IOException
     */
    public String writeFile(String file_path, Set<DataPipe> rows) throws IOException {
        File f=new File(file_path);
        if(!new File(f.getParent()).exists()){
            new File(f.getParent()).mkdirs();
        }
        FileUtil.clear(f);
        for (DataPipe line:rows){
            String l = line.generateString();
            FileUtil.appendString(f, l);
        }
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
            FileUtil.clear(f);
            return f.getAbsolutePath();
        }catch (Exception e){

        }
        return "";
    }

    public String writeFtpFile(String file_path, SFTPUtil sftpUtil) throws IOException, SftpException {
        File f=new File(file_path);
        sftpUtil.login();
        String direct = removeLastComponent(file_path);
        sftpUtil.mkdirs(direct);
        System.out.println(file_path);
        System.out.println(direct);
        sftpUtil.upload(direct, file_path);
        sftpUtil.logout();
        return f.getAbsolutePath();
    }

    private static String removeLastComponent(String path) {
        int lastIndex = path.lastIndexOf('/');
        if (lastIndex != -1) {
            return path.substring(0, lastIndex);
        }
        return path;
    }

    public void setStatus(String task_id,String status){
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyInstance strategyInstance=new StrategyInstance();
        strategyInstance.setId(task_id);
        strategyInstance.setStatus(status);
        strategyInstance.setUpdate_time(new Timestamp(System.currentTimeMillis()));
        strategyInstanceService.updateByPrimaryKeySelective(strategyInstance);
    }


    /**
     * 解析上游任务和当前节点任务做运算-入口
     * @param label_use_type offline: 离线处理,online:在线处理
     * @param currentRows
     * @param is_disenable
     * @param file_dir
     * @param param
     * @param run_jsmind_data
     * @param strategyInstanceService
     * @return
     * @throws IOException
     */
    public Set<DataPipe> calculateCommon(StrategyLogInfo strategyLogInfo,String label_use_type, Set<DataPipe> currentRows, String is_disenable, String file_dir, Map param, Map run_jsmind_data, StrategyInstanceServiceImpl strategyInstanceService) throws Exception {
        String pre_tasks = param.get("pre_tasks").toString();
        List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
        String operate=run_jsmind_data.get("operate").toString();
        String status=run_jsmind_data.getOrDefault("data_status",Const.FILE_STATUS_SUCCESS).toString();//依赖数据状态,1:成功,2:失败,3:不区分
        List<String> pre_tasks_list = Lists.newArrayList();
        if(!StringUtils.isEmpty(pre_tasks)){
            pre_tasks_list = Lists.newArrayList(pre_tasks.split(","));
        }
        if(label_use_type.equalsIgnoreCase("offline")){
            return calculate(strategyLogInfo, file_dir, pre_tasks_list, operate, currentRows, strategyInstances, is_disenable, status);
        }else if(label_use_type.equalsIgnoreCase("online")){
            return calculatePreTasksByOnlineLabel(strategyLogInfo, pre_tasks_list ,file_dir, operate, strategyInstances, status);
        }
        throw new Exception("不支持的标签类型");
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
    public Set<DataPipe> calculate(StrategyLogInfo strategyLogInfo, String file_dir, List<String> pre_tasks, String operate, Set<DataPipe> cur_rows, List<StrategyInstance> strategyInstances,
                                 String is_disenable, String status) throws Exception {

        //无上游,则直接返回当前结果集
        if(pre_tasks==null || pre_tasks.size()== 0){
            return cur_rows.parallelStream().map(s->{
                s.setTask_type(strategyLogInfo.getInstance_type());
                s.setStatus(Const.FILE_STATUS_SUCCESS);
                return s;
            }).collect(Collectors.toSet());
            //return cur_rows.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(new HashMap<>()).build()).collect(Collectors.toSet());
        }

        //指定不使用上游数据,返回当前结果集
        if(operate.equalsIgnoreCase("not_use")){
            return cur_rows.parallelStream().map(s->{
                s.setTask_type(strategyLogInfo.getInstance_type());
                s.setStatus(Const.FILE_STATUS_SUCCESS);
                return s;
            }).collect(Collectors.toSet());
            //return cur_rows.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(new HashMap<>()).build()).collect(Collectors.toSet());
        }

        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
        }

        Set<String> cur_rows_set = Sets.newHashSet(cur_rows.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet()));

        // 临时结果存储透传参数
        Map<String, Map<String, Object>> ext = new ConcurrentHashMap<>();

        Set<String> result=Sets.newHashSet();

        //如果是排除逻辑, 上游多个任务先取交集,然后再排除当前标签
        if(operate.equalsIgnoreCase("not")){
            //取所有上游的交集
            for(String task:pre_tasks){
                List<DataPipe> lines = readFile(file_dir, task, status, split);
                loadExt(lines, ext);
                Set<String> set = Sets.newHashSet(lines.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet()));
                result = Sets.intersection(result, set);
            }
            if(is_disenable.equalsIgnoreCase("false")){
                result = Sets.difference(result, cur_rows_set);
            }
        }else if(operate.equalsIgnoreCase("and")){
            //交集
            result = null;
            for(String task:pre_tasks){
                //List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                List<DataPipe> lines = readFile(file_dir, task, status, split);
                loadExt(lines, ext);
                Set<String> set = Sets.newHashSet(lines.stream().map(s->s.getUdata()).collect(Collectors.toSet()));
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
                result = Sets.intersection(result, cur_rows_set);
            }
        }else if(operate.equalsIgnoreCase("or")){
            //取并集去重
            result = null;
            for(String task:pre_tasks){
                //List<String> lines = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
                List<DataPipe> lines = readFile(file_dir, task, status, split);
                loadExt(lines, ext);
                Set<String> set = Sets.newHashSet(lines.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet()));
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
                result = Sets.intersection(result, cur_rows_set);
            }
        }else if(operate.equalsIgnoreCase("not_use")){
            //见最上方操作
        }

        loadExt(Lists.newArrayList(cur_rows), ext);

        return result.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(ext.getOrDefault(s, new HashMap<>())).build()).collect(Collectors.toSet());
    }

    public void loadExt(List<DataPipe> lines, Map<String, Map<String, Object>> ext){
        lines.parallelStream().forEach(s->{
            Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(s.getExt());
            Map<String, Object> old = ext.getOrDefault(s.getUdata(), new HashMap<>());
            old.putAll(stringObjectMap);
            ext.put(s.getUdata(), old);
        });
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
    public Set<DataPipe> calculate(StrategyLogInfo strategyLogInfo, List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances, String status) throws Exception {
        if(operate.equalsIgnoreCase("not_use")){
            return Sets.newHashSet();
        }

        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
        }

        // 临时结果存储透传参数
        Map<String, Map<String, Object>> ext = new ConcurrentHashMap<>();

        Set<String> result=Sets.newHashSet();

        //如果是排除逻辑,需要先找一个base数据基于这个数据做排除
        if(operate.equalsIgnoreCase("not")){
            //需要先找到一个base
            throw new Exception("运算符插件-不支持not, not_use 用法");

        }

        //多个任务交并排逻辑
        for(String task:pre_tasks){
            if(map.get(task).getStatus().equalsIgnoreCase("skip")){
                //continue;
            }
            //List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
            List<DataPipe> rows = readFile(file_dir, task, status, split);
            loadExt(rows, ext);
            Set<String> set=Sets.newHashSet(rows.stream().map(s->s.getUdata()).collect(Collectors.toSet()));
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

        return result.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(ext.getOrDefault(s, new HashMap<>())).build()).collect(Collectors.toSet());

    }


    /**
     *  人查值标签 使用
     * @param pre_tasks
     * @param file_dir
     * @param operate
     * @param strategyInstances
     * @param status
     * @return
     * @throws IOException
     */
    public Set<DataPipe> calculatePreTasksByOnlineLabel(StrategyLogInfo strategyLogInfo, List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances, String status) throws Exception {
        if(operate.equalsIgnoreCase("not_use")){
            return Sets.newHashSet();
        }

        Map<String,StrategyInstance> map=new HashMap<>();
        for (StrategyInstance strategyInstance: strategyInstances){
            map.put(strategyInstance.getId(), strategyInstance);
        }

        if(pre_tasks.size()==0){
            throw new Exception("使用实时标签,必须存在上游标签");
        }

        // 临时结果存储透传参数
        Map<String, Map<String, Object>> ext = new ConcurrentHashMap<>();

        Set<String> result=Sets.newHashSet();

        //多个任务交并排逻辑
        for(String task:pre_tasks){
            //List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
            List<DataPipe> rows = readFile(file_dir, task, status, split);
            loadExt(rows, ext);
            Set<String> set=Sets.newHashSet(rows.stream().map(s->s.getUdata()).collect(Collectors.toSet()));
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
                    //此处使用交集
                    result = Sets.intersection(result, set);
                }else if(operate.equalsIgnoreCase("not_use")){
                    return Sets.newHashSet();
                }
            }
        }
        return result.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).build()).collect(Collectors.toSet());
    }

    /**
     * 统一写入文件并打印日志
     * @param strategyLogInfo
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLogAndUpdateStatus2Finish(StrategyLogInfo strategyLogInfo,  Set<DataPipe> rs) throws Exception {
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
    public void writeFileAndPrintLogAndUpdateStatus2Finish(String id,String strategy_id, String file_path, Set<DataPipe> rs) throws Exception {
        String save_path = writeFile(file_path, rs);
        //失败-重试3次
        int retry = 0;
        while (true){
            try{
                if(retry > 3){
                    String logStr = StrUtil.format("task: {}, 上传ftp失败, file: {}", id, file_path);
                    LogUtil.info(strategy_id, id, logStr);
                    throw new Exception("上传ftp失败");
                }

                if(checkSftp()){
                    writeFtpFile(file_path, sftpUtil);
                    break;
                }else if(storageMode().equalsIgnoreCase("minio")){
                    //写入对象存储
                    MinioUtil.putObject(minioClient, getBucket(), getRegion(), "application/octet-stream",file_path, file_path, null);
                    break;
                }else{
                    break;
                }
            }catch (Exception e){
                logger.error("label server writeFileAndPrintLogAndUpdateStatus2Finish error: ", e);
                if(retry > 3){
                    throw e;
                }
                retry+=1;
            }
        }

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
    public void writeRocksdb(String file_rocksdb_path, String id, Set<DataPipe> rs, String status) throws Exception {
        File file = new File(file_rocksdb_path);
        if(!file.exists()){
            file.mkdirs();
        }
        RocksDB rocksDB = RocksDBUtil.getConnection(file_rocksdb_path);
        if(rs != null && rs.size() > 0){
            for (DataPipe r: rs){
                String key = r.getUdata()+"_"+id;
                rocksDB.put(key.getBytes(), status.getBytes());
            }
        }
        rocksDB.close();
    }


    /**
     * 读取本地文件或者ftp文件内容
     * 优先读取本地文件
     * @param file_dir
     * @param task
     * @param status
     * @param split 默认\t
     * @return
     * @throws Exception
     */
    public List<DataPipe> readFile(String file_dir, String task, String status, String split) throws Exception {
        List<DataPipe> rows = new ArrayList<>();
        if(cn.hutool.core.io.FileUtil.exist(file_dir+"/"+task)){
            rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status, split);
        }else{
            if(checkSftp()){
                sftpUtil.login();
                byte[] bytes = sftpUtil.download(file_dir, task);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                List<String> tmp = IOUtils.readLines(bais, "utf-8");
                bais.close();
                for (String line: tmp){
                    DataPipe dataPipe = DataPipe.readStringSplit(line, split);
                    if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                        rows.add(dataPipe);
                    }else{
                        if(dataPipe.getStatus().equalsIgnoreCase(status)){
                            rows.add(dataPipe);
                        }
                    }
                }

            }else if(storageMode().equalsIgnoreCase("minio")){
                //minio对象存储
                InputStream inputStream = MinioUtil.getObject(minioClient, getBucket(), getRegion(), file_dir+"/"+task);
                List<String> tmp = IOUtils.readLines(inputStream, "utf-8");
                inputStream.close();
                for (String line: tmp){
                    DataPipe dataPipe = DataPipe.readStringSplit(line, split);
                    if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                        rows.add(dataPipe);
                    }else{
                        if(dataPipe.getStatus().equalsIgnoreCase(status)){
                            rows.add(dataPipe);
                        }
                    }
                }

            }else{
                throw new Exception("无法找到对应的数据文件");
            }
        }
        return rows;
    }

    public void removeTask(String key){
        if(!StringUtils.isEmpty(key)){
            LabelServer.tasks.remove(key);
        }
    }

    public static Map<String, Object> getJinJavaParam(Timestamp cur_time) {
        String msg = "目前支持日期参数以下模式: {{zdh_date}} => yyyy-MM-dd ,{{zdh_date_nodash}}=> yyyyMMdd " +
                ",{{zdh_date_time}}=> yyyy-MM-dd HH:mm:ss,{{zdh_year}}=> yyyy年,{{zdh_month}}=> 月,{{zdh_day}}=> 日," +
                "{{zdh_hour}}=>24小时制,{{zdh_minute}}=>分钟,{{zdh_second}}=>秒,{{zdh_time}}=>时间戳, 更多参数可参考【系统内置参数】点击链接查看具体使用例子";

        String date_nodash = DateUtil.formatNodash(cur_time);
        String date_time = DateUtil.formatTime(cur_time);
        String date_dt = DateUtil.format(cur_time);
        Map<String, Object> jinJavaParam = new HashMap<>();
        jinJavaParam.put("zdh_date_nodash", date_nodash);
        jinJavaParam.put("zdh_date_time", date_time);
        jinJavaParam.put("zdh_date", date_dt);
        jinJavaParam.put("zdh_year", DateUtil.year(cur_time));
        jinJavaParam.put("zdh_month", DateUtil.month(cur_time));
        jinJavaParam.put("zdh_day", DateUtil.day(cur_time));
        jinJavaParam.put("zdh_hour", DateUtil.hour(cur_time));
        jinJavaParam.put("zdh_minute", DateUtil.minute(cur_time));
        jinJavaParam.put("zdh_second", DateUtil.second(cur_time));
        jinJavaParam.put("zdh_monthx", DateUtil.monthx(cur_time));
        jinJavaParam.put("zdh_dayx", DateUtil.dayx(cur_time));
        jinJavaParam.put("zdh_hourx", DateUtil.hourx(cur_time));
        jinJavaParam.put("zdh_minutex", DateUtil.minutex(cur_time));
        jinJavaParam.put("zdh_secondx", DateUtil.secondx(cur_time));

        jinJavaParam.put("zdh_time", cur_time.getTime());

        jinJavaParam.put("zdh_dt", new DateUtil());
        return jinJavaParam;

    }

    /**
     * 获取公共参数,必须提前执行initJinJavaCommonParam函数
     * @return
     */
    public Map<String, Object> getJinJavaCommonParam(){
        return this.jinJavaCommonParam;
    }
}
