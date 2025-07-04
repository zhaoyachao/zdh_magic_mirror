package com.zyc.magic_mirror.label.calculate.impl;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jcraft.jsch.SftpException;
import com.zyc.magic_mirror.common.entity.CrowdFileInfo;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.StrategyLogInfo;
import com.zyc.magic_mirror.common.util.*;
import com.zyc.magic_mirror.label.service.impl.CrowdFileServiceImpl;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 人群文件任务
 */
public class CrowdFileCalculateImpl extends BaseCalculate{
    private static Logger logger= LoggerFactory.getLogger(CrowdFileCalculateImpl.class);

    /**
     * "id" : 1032062601107869696,
     * 		"strategy_context" : "测试新标签",
     * 		"group_id" : "测试策略组",
     * 		"group_context" : "测试策略组",
     * 		"group_instance_id" : "1032062598209605632",
     * 		"instance_type" : "crowd_file",
     * 		"start_time" : "2022-10-18 22:48:16",
     * 		"end_time" : null,
     * 		"jsmind_data" : "{\"crowd_rule_context\":\"测试新标签\",\"type\":\"crowd_rule\",\"time_out\":\"86400\",\"positionX\":303,\"positionY\":78,\"operate\":\"or\",\"crowd_rule\":\"985279445663223808\",\"touch_type\":\"database\",\"name\":\"测试新标签\",\"more_task\":\"crowd_rule\",\"id\":\"5e7_0c1_8f31_4a\",\"divId\":\"5e7_0c1_8f31_4a\",\"depend_level\":\"0\"}",
     * 		"owner" : "zyc",
     * 		"is_delete" : "0",
     * 		"create_time" : "2022-06-11 21:38:40",
     * 		"update_time" : "2022-10-18 22:48:18",
     * 		"expr" : null,
     * 		"misfire" : "0",
     * 		"priority" : "",
     * 		"status" : "create",
     * 		"quartz_time" : null,
     * 		"use_quartz_time" : null,
     * 		"time_diff" : null,
     * 		"schedule_source" : "2",
     * 		"cur_time" : "2022-10-18 22:48:16",
     * 		"run_time" : "2022-10-18 22:48:19",
     * 		"run_jsmind_data" : "{\"crowd_rule_context\":\"测试新标签\",\"type\":\"crowd_file\",\"time_out\":\"86400\",\"positionX\":303,\"positionY\":78,\"operate\":\"or\",\"crowd_rule\":\"985279445663223808\",\"touch_type\":\"database\",\"name\":\"测试新标签\",\"more_task\":\"crowd_rule\",\"id\":\"5e7_0c1_8f31_4a\",\"divId\":\"5e7_0c1_8f31_4a\",\"depend_level\":\"0\"}",
     * 		"next_tasks" : "1032062601112064000",
     * 		"pre_tasks" : "1032062601124646912",
     * 		"is_disenable" : "false",
     * 		"depend_level" : "0",
     * 		"touch_type" : "database"
     *
     * run_jsmind_data 结构
     * {
     * 	"rule_expression_cn": "f1",
     * 	"type": "crowd_file",
     * 	"time_out": "86400",
     * 	"positionX": 237,
     * 	"positionY": 279,
     * 	"is_base": "false",
     * 	"operate": "and",
     * 	"touch_type": "database",
     * 	"crowd_file_context": "f1",
     * 	"name": "f1",
     * 	"more_task": "crowd_file",
     * 	"crowd_file": "1",
     * 	"id": "f5e_9f8_ad7a_88",
     * 	"divId": "f5e_9f8_ad7a_88",
     * 	"depend_level": "0"
     * }
     * @param param
     * @param atomicInteger
     * @param dbConfig
     */
    public CrowdFileCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
    public void process() {
        super.process();
        String logStr="";
        try{
            //客群运算id
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //以文件id作为文件名
            String rule_id=run_jsmind_data.get("rule_id").toString();

            Set<DataPipe> rowsStr=Sets.newHashSet();
            Set<DataPipe> rs=Sets.newHashSet() ;
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //根据rule_id查询文件信息
                CrowdFileServiceImpl crowdFileService = new CrowdFileServiceImpl();
                CrowdFileInfo crowdFileInfo = crowdFileService.selectById(rule_id);

                //下载sftp文件存储本地
                String file_sftp_path = getFilePath(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                        strategyLogInfo.getStrategy_group_instance_id(), "sftp_"+strategyLogInfo.getStrategy_instance_id()+"_"+crowdFileInfo.getFile_name());

                File file = new File(file_sftp_path);
                Files.createParentDirs(file);

                //获取人群文件,sftp
                String sftp_enable = dbConfig.getOrDefault("sftp.enable", "");
                String storage_mode = dbConfig.getOrDefault("storage.mode", "");
                String local_path = dbConfig.getOrDefault("file.path", "");

                if(sftp_enable.equalsIgnoreCase("true")){
                    sftpDownload(strategyLogInfo, crowdFileInfo.getFile_name(), file_sftp_path);
                }else if(storage_mode.equalsIgnoreCase("minio")){
                    minioDownload(strategyLogInfo, crowdFileInfo.getFile_name(), file_sftp_path);
                }else{
                    //本地文件
                    String local_file = local_path+"/crowd_file/"+crowdFileInfo.getFile_name();
                    Files.copy(new File(local_file), new File(file_sftp_path));
                }

                List<DataPipe> rows = new ArrayList<>();
                //读取本地文件
                if(crowdFileInfo.getFile_name().endsWith("xlsx")){
                    //excel 文件
                    rows = FileUtil.readExcelSplit(new File(file_sftp_path),"xlsx", Charset.forName("utf-8"), Const.FILE_STATUS_ALL, true);
                }else if(crowdFileInfo.getFile_name().endsWith("xls")){
                    rows = FileUtil.readExcelSplit(new File(file_sftp_path),"xls", Charset.forName("utf-8"), Const.FILE_STATUS_ALL, true);
                }else{
                    rows = FileUtil.readTextSplit(new File(file_sftp_path), Charset.forName("utf-8"), "\t", true);
                }
                rowsStr = Sets.newHashSet(rows);

            }

            String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                    strategyLogInfo.getStrategy_group_instance_id());
            //解析上游任务并和当前节点数据做运算
            rs = calculateCommon(strategyLogInfo,"offline",rowsStr, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            logger.error("label crowdfile run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }

    private void sftpDownload(StrategyLogInfo strategyLogInfo, String fileName, String saveLocalFilePath) throws FileNotFoundException, SftpException {
        String path = dbConfig.get("sftp.path")+"/crowd_file";
        String username=dbConfig.get("sftp.username");
        String password=dbConfig.get("sftp.password");
        String host=dbConfig.get("sftp.host");
        int port=Integer.parseInt(dbConfig.get("sftp.port"));
        SFTPUtil sftpUtil=new SFTPUtil(username, password, host, port);
        //此处直接使用directory目录是有风险的,人群文件最好单独设置一个目录,不和ftp的根目录共用
        sftpUtil.download(path, fileName, saveLocalFilePath);
    }

    private void minioDownload(StrategyLogInfo strategyLogInfo, String fileName, String saveLocalFilePath) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InsufficientDataException, InternalException {
        MinioClient minioClient = getMinioClient();
        String objectName = strategyLogInfo.getBase_path()+"/crowd_file/"+fileName;
        InputStream inputStream = MinioUtil.getObject(minioClient, getBucket(), getRegion(), objectName);
        File file = new File(saveLocalFilePath);

        if(!file.getParentFile().exists()){
            file.getParentFile().mkdirs();
        }
        Files.asByteSink(new File(saveLocalFilePath)).writeFrom(inputStream);
    }
}
