package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcraft.jsch.SftpException;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.*;
import com.zyc.plugin.PluginServer;
import com.zyc.plugin.calculate.CalculateCommomParam;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseCalculate {
    private static Logger logger= LoggerFactory.getLogger(BaseCalculate.class);

    public static ThreadLocal<String> localVar = new ThreadLocal<>();
    public static ConcurrentHashMap<String,BlockingQueue> queue = new ConcurrentHashMap<String, BlockingQueue>();

    public abstract String getOperate(Map run_jsmind_data);


    private SFTPUtil sftpUtil;

    private MinioClient minioClient;

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
        strategyLogInfo.setCur_time(Timestamp.valueOf(cur_time));
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
        String id=param.get("id").toString();
        return getFilePath(getFileDir(base_path,group_id,group_instance_id), id);
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
    public CalculateResult calculateResult(String base_path, Map run_jsmind_data, Map param, StrategyInstanceServiceImpl strategyInstanceService) throws Exception {
        //生成参数
        Set<String> rs=Sets.newHashSet() ;
        //检查标签上游
        CalculateCommomParam calculateCommomParam = resovlePreTask(param, strategyInstanceService);
        String operate = getOperate(run_jsmind_data);
        String status=run_jsmind_data.getOrDefault("data_status",Const.FILE_STATUS_SUCCESS).toString();//依赖数据状态,1:成功,2:失败,3:不区分
        String file_dir= getFileDir(base_path,calculateCommomParam.getGroup_id(),calculateCommomParam.getGroup_instance_id());
        rs = calculatePreTasks(calculateCommomParam.getPre_tasks(), file_dir, operate,calculateCommomParam.getStrategyInstanceList(), status);

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
    public Set<String> calculatePreTasks(String pre_tasks, String file_dir, String operator, List<StrategyInstance> strategyInstances, String status) throws Exception {
        Set<String> rs=Sets.newHashSet() ;
        if(!StringUtils.isEmpty(pre_tasks)){
            rs = calculate(Lists.newArrayList(pre_tasks.split(",")), file_dir, operator, strategyInstances, status);
        }else{
            //如果上游为空,则过滤无效 todo
        }
        return rs;
    }


    /**
     * 返回文件绝对路径
     * @param file_path
     * @param rows
     * @return
     * @throws IOException
     */
    public String writeFile(String file_path, Set<String> rows) throws IOException {
        File f=new File(file_path);
        if(!new File(f.getParent()).exists()){
            new File(f.getParent()).mkdirs();
        }

        FileUtil.clear(f);
        for (String line:rows){
            if(!line.contains(",")){
                line = line+","+Const.FILE_STATUS_SUCCESS;
            }
            FileUtil.appendString(f, line);
        }
        return f.getAbsolutePath();
    }

    public String appendFileByError(String task_id, String file_path, Set<String> rows) throws IOException {
        File f=new File(file_path);
        if(!new File(f.getParent()).exists()){
            new File(f.getParent()).mkdirs();
        }

        for (String line:rows){
            FileUtil.appendString(f, line+","+Const.FILE_STATUS_FAIL);
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
            logger.error("plugin server writeEmptyFile error: ", e);
        }
        return "";
    }

    public String writeFtpFile(String file_path, SFTPUtil sftpUtil) throws IOException, SftpException {
        File f=new File(file_path);
        sftpUtil.login();
        String direct = removeLastComponent(file_path);
        sftpUtil.mkdirs(direct);
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

    public List<String> readHisotryFile(String file_dir, String task, String status) throws Exception {
        List<String> rows = new ArrayList<>();
        String file_path = file_dir + "/" + task;
        if(cn.hutool.core.io.FileUtil.exist(file_path)){
            File f=new File(file_path);
            if(f.exists() && f.isFile()){
                rows = FileUtil.readStringSplit(f, Charset.forName("utf-8"),Const.FILE_STATUS_ALL);
                return rows;
            }
        }else{
            if(checkSftp()){
                sftpUtil.login();
                byte[] bytes = sftpUtil.download(file_dir, task);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                List<String> tmp = IOUtils.readLines(bais, "utf-8");

                for (String line: tmp){
                    String[] row = line.split(",");
                    if(row.length>2){
                        if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                            rows.add(row[0]);
                        }else{
                            if(row[1].equalsIgnoreCase(status)){
                                rows.add(row[0]);
                            }
                        }
                    }else{
                        rows.add(row[0]);
                    }
                }
                return rows;
            }
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
    public Set<String> calculate(List<String> pre_tasks, String file_dir, String operate, List<StrategyInstance> strategyInstances, String status) throws Exception {

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
            //List<String> rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
            List<String> rows = readFile(file_dir, task, status);
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
                    throw new Exception("不支持排除操作,plugin模块仅支持and, or, not_use操作符");
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
     * @param strategyLogInfo
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLogAndUpdateStatus2Finish(StrategyLogInfo strategyLogInfo,  Set<String> rs, Set<String> rs_error) throws Exception {
        String logStr = StrUtil.format("task: {}, calculate finish size: {}", strategyLogInfo.getStrategy_instance_id(), rs.size());
        LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
        writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo.getStrategy_instance_id(),strategyLogInfo.getStrategy_id(), strategyLogInfo.getFile_path(), rs, rs_error);
    }

    /**
     * 统一写入文件并打印日志,更新状态为完成
     * @param id
     * @param strategy_id
     * @param file_path
     * @param rs
     * @throws IOException
     */
    public void writeFileAndPrintLogAndUpdateStatus2Finish(String id,String strategy_id, String file_path, Set<String> rs, Set<String> rs_error) throws Exception {
        String save_path = writeFile(file_path, rs);
        appendFileByError(id,file_path, rs_error);
        String logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
        LogUtil.info(strategy_id, id, logStr);

        //失败-重试3次
        int retry = 0;
        while (true){
            try{
                if(retry > 3){
                    logStr = StrUtil.format("task: {}, 上传ftp失败, file: {}", id, file_path);
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

        setStatus(id, Const.STATUS_FINISH);
        logStr = StrUtil.format("task: {}, update status finish", id);
        LogUtil.info(strategy_id, id, logStr);
    }

    public void writeFileAndPrintLog(StrategyLogInfo strategyLogInfo,  Set<String> rs) throws Exception {
        String save_path = writeFile(strategyLogInfo.getFile_path(), rs);

        //失败-重试3次
        int retry = 0;
        while (true){
            try{
                if(retry > 3){
                    String logStr = StrUtil.format("task: {}, 上传ftp失败, file: {}", strategyLogInfo.getStrategy_instance_id(), save_path);
                    LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    throw new Exception("上传ftp失败");
                }

                if(checkSftp()){
                    writeFtpFile(strategyLogInfo.getFile_path(), sftpUtil);
                    break;
                }else if(storageMode().equalsIgnoreCase("minio")){
                    //写入对象存储
                    MinioUtil.putObject(minioClient, getBucket(), getRegion(), "application/octet-stream",strategyLogInfo.getFile_path(), strategyLogInfo.getFile_path(), null);
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

        String logStr = StrUtil.format("task: {}, write finish, file: {}", strategyLogInfo.getStrategy_instance_id(), save_path);
        LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
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


    /**
     * 读取本地文件或者ftp文件内容
     * 优先读取本地文件
     * @param file_dir
     * @param task
     * @param status
     * @return
     * @throws Exception
     */
    public List<String> readFile(String file_dir, String task, String status) throws Exception {
        List<String> rows = new ArrayList<>();
        if(cn.hutool.core.io.FileUtil.exist(file_dir+"/"+task)){
            rows = FileUtil.readStringSplit(new File(file_dir+"/"+task), Charset.forName("utf-8"), status);
        }else{
            if(checkSftp()){
                sftpUtil.login();
                byte[] bytes = sftpUtil.download(file_dir, task);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                List<String> tmp = IOUtils.readLines(bais, "utf-8");
                bais.close();
                for (String line: tmp){
                    String[] row = line.split(",");
                    if(row.length>2){
                        if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                            rows.add(row[0]);
                        }else{
                            if(row[1].equalsIgnoreCase(status)){
                                rows.add(row[0]);
                            }
                        }
                    }else{
                        rows.add(row[0]);
                    }
                }

            }else if(storageMode().equalsIgnoreCase("minio")){
                //minio对象存储
                InputStream inputStream = MinioUtil.getObject(minioClient, getBucket(), getRegion(), file_dir+"/"+task);
                List<String> tmp = IOUtils.readLines(inputStream, "utf-8");
                inputStream.close();
                for (String line: tmp){
                    String[] row = line.split(",");
                    if(row.length>2){
                        if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                            rows.add(row[0]);
                        }else{
                            if(row[1].equalsIgnoreCase(status)){
                                rows.add(row[0]);
                            }
                        }
                    }else{
                        rows.add(row[0]);
                    }
                }

            }else{
                throw new Exception("无法找到对应的数据文件");
            }
        }
        return rows;
    }

    public List<String> readFile(String file_path) throws Exception {
        List<String> rows = new ArrayList<>();
        if(cn.hutool.core.io.FileUtil.exist(file_path)){
            rows = FileUtil.readString(new File(file_path), Charset.forName("utf-8"));
        }else{
            if(checkSftp()){
                sftpUtil.login();
                String file_dir = cn.hutool.core.io.FileUtil.getParent(file_path, 1);
                String file_name = cn.hutool.core.io.FileUtil.getName(file_path);
                byte[] bytes = sftpUtil.download(file_dir, file_name);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                rows = IOUtils.readLines(bais, "utf-8");
                bais.close();
            }else if(storageMode().equalsIgnoreCase("minio")){
                //minio对象存储
                InputStream inputStream = MinioUtil.getObject(minioClient, getBucket(), getRegion(), file_path);
                rows = IOUtils.readLines(inputStream, "utf-8");
                inputStream.close();
            }else{
               
            }
        }
        return rows;
    }

    public void removeTask(String key){
        if(!StringUtils.isEmpty(key)){
            PluginServer.tasks.remove(key);
        }

    }


    /**
     * 获取变量池信息,并合并到params集合中
     * @param strategy_group_instance_id
     * @param params
     */
    public void mergeMapByVarPool(String strategy_group_instance_id, Map<String, Object> params){
        Map<Object, Object> varPoolMap = getVarPoolBy(strategy_group_instance_id);

        if(varPoolMap != null && varPoolMap.size() > 0){
            for(Map.Entry entry: varPoolMap.entrySet()){
                String key = entry.getKey().toString();
                Object value = entry.getKey();
                params.put(key, value);
            }
        }
    }


    public Map<Object, Object> getVarPoolBy(String strategy_group_instance_id){
        String key = "varpool:gid"+strategy_group_instance_id;
        if(JedisPoolUtil.redisClient().isExists(key)){
            return JedisPoolUtil.redisClient().hGetAll(key);
        }
        return new HashMap<>();
    }

    public void addStrategy2VarPool(String strategy_group_instance_id, String varpool_domain, String varpool_code, Object value){
        String key = "varpool:gid"+strategy_group_instance_id;
        String secondKey = varpool_domain+":"+varpool_code;
        JedisPoolUtil.redisClient().hSet(key, secondKey, value);
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
}
