package com.zyc.label.calculate.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.FileUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.SFTPUtil;
import com.zyc.label.calculate.CrowdFileCalculate;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 人群文件任务
 */
public class CrowdFileCalculateImpl extends BaseCalculate implements CrowdFileCalculate {
    private static Logger logger= LoggerFactory.getLogger(CrowdFileCalculateImpl.class);
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    /**
     * "id" : 1032062601107869696,
     * 		"strategy_context" : "测试新标签",
     * 		"group_id" : "测试策略组",
     * 		"group_context" : "测试策略组",
     * 		"group_instance_id" : "1032062598209605632",
     * 		"instance_type" : "crowd_rule",
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
     * 		"run_jsmind_data" : "{\"crowd_rule_context\":\"测试新标签\",\"type\":\"crowd_rule\",\"time_out\":\"86400\",\"positionX\":303,\"positionY\":78,\"operate\":\"or\",\"crowd_rule\":\"985279445663223808\",\"touch_type\":\"database\",\"name\":\"测试新标签\",\"more_task\":\"crowd_rule\",\"id\":\"5e7_0c1_8f31_4a\",\"divId\":\"5e7_0c1_8f31_4a\",\"depend_level\":\"0\"}",
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
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
        getSftpUtil(this.dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        String logStr="";
        try{

            //客群运算id
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //以文件id作为文件名
            String rule_id=run_jsmind_data.get("rule_id").toString();

            Set<String> rowsStr=Sets.newHashSet();
            Set<String> rs=Sets.newHashSet() ;
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //获取人群文件,sftp
                String directory = dbConfig.get("sftp.path");
                String username=dbConfig.get("sftp.username");
                String password=dbConfig.get("sftp.password");
                String host=dbConfig.get("sftp.host");
                int port=Integer.parseInt(dbConfig.get("sftp.port"));
                SFTPUtil sftpUtil=new SFTPUtil(username, password, host, port);
                //下载sftp文件存储本地
                String file_sftp_path = getFilePath(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                        strategyLogInfo.getStrategy_group_instance_id(), "sftp_"+strategyLogInfo.getStrategy_instance_id());
                //此处直接使用directory目录是有风险的,人群文件最好单独设置一个目录,不和ftp的根目录共用
                sftpUtil.download(directory, rule_id, file_sftp_path);

                //读取本地文件
                List<String> rows = FileUtil.readStringSplit(new File(file_sftp_path), Charset.forName("utf-8"), Const.FILE_STATUS_SUCCESS);
                rowsStr = Sets.newHashSet(rows);
            }

            String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                    strategyLogInfo.getStrategy_group_instance_id());
            //解析上游任务并和当前节点数据做运算
            rs = calculateCommon("offline",rowsStr, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);


            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            logger.error("label crowdfile run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }

    }
}
