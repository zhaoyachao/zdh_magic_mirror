package com.zyc.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.FileUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.common.util.SFTPUtil;
import com.zyc.label.calculate.CrowdFileCalculate;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
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
        String file_path = "";
        try{
            String base_path=dbConfig.get("file.path");
            //客群运算id
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //以文件id作为文件名
            String crowd_file=run_jsmind_data.get("crowd_file").toString();

            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("客群运算信息数据库配置异常");
            }
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
                String file_sftp_path = getFilePath(base_path, group_id, group_instance_id, "sftp_"+id);
                sftpUtil.download(directory, crowd_file, file_sftp_path);

                //读取本地文件
                List<String> rows = FileUtil.readStringSplit(new File(file_sftp_path), Charset.forName("utf-8"));
                rowsStr = Sets.newHashSet(rows);
            }
            //获取上游任务
            String pre_tasks = this.param.get("pre_tasks").toString();
            String file_dir= getFileDir(base_path,group_id,group_instance_id);
            String operate=run_jsmind_data.get("operate").toString();
            rs=Sets.newHashSet() ;
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
            List<String> pre_tasks_list = Lists.newArrayList();
            if(!StringUtils.isEmpty(pre_tasks)){
                pre_tasks_list = Lists.newArrayList(pre_tasks.split(","));
            }
            rs = calculate(file_dir, pre_tasks_list, operate, rowsStr, strategyInstances, is_disenable);

            logStr = StrUtil.format("task: {}, calculate finish size: {}", id, rs.size());
            LogUtil.info(strategy_id, id, logStr);
            file_path = getFilePath(base_path,group_id,group_instance_id,id);

            String save_path = writeFile(id,file_path, rs);
            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(strategy_id, id, logStr);
            setStatus(id, "finish");
            logStr = StrUtil.format("task: {}, update status finish", id);
            LogUtil.info(strategy_id, id, logStr);

        }catch (Exception e){
            atomicInteger.decrementAndGet();
            writeEmptyFile(file_path);
            setStatus(id, "error");
            LogUtil.error(strategy_id, id, e.getMessage());
            e.printStackTrace();
        }

    }
}
