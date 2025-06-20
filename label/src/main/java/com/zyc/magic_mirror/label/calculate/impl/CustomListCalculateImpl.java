package com.zyc.magic_mirror.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hubspot.jinjava.Jinjava;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 用户名单实现
 */
public class CustomListCalculateImpl extends BaseCalculate implements Runnable {
    private static Logger logger= LoggerFactory.getLogger(CustomListCalculateImpl.class);

    /**
     * {
     * 	"owner": "zyc",
     * 	"schedule_source": "2",
     * 	"strategy_context": "(年龄 in 19)",
     * 	"create_time": 1658629372000,
     * 	"jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *        },
     * 	"run_time": 1660993147000,
     * 	"group_instance_id": "1010624036146778112",
     * 	"cur_time": 1660993145000,
     * 	"pre_tasks": "",
     * 	"group_context": "测试策略组",
     * 	"priority": "",
     * 	"is_disenable": "false",
     * 	"is_delete": "0",
     * 	"run_jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *    },
     * 	"start_time": 1660993145000,
     * 	"update_time": 1660993147000,
     * 	"group_id": "测试策略组",
     * 	"misfire": "0",
     * 	"next_tasks": "1010624036201304064",
     * 	"id": "1010624036209692673",
     * 	"instance_type": "label",
     * 	"depend_level": "0",
     * 	"status": "create"
     * }
     */

    public CustomListCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        String logStr="";
        try{

            //获取标签code
            String rule_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            Set<DataPipe> cur_rows = Sets.newHashSet();
            Set<String> rowsStr = Sets.newHashSet();
            //判断是否跳过类的策略,通过is_disenable=true,禁用的任务直接拉取上游任务的结果,并集(),交集(),排除()
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //生成参数
                String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
                Map<String, Object> commonParam = getJinJavaCommonParam();
                Jinjava jinjava = new Jinjava();
                name_list_str = jinjava.render(name_list_str, commonParam);

                logStr = StrUtil.format("task: {}, param: {}", strategyLogInfo.getStrategy_instance_id(), name_list_str);
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                String[] name_list = name_list_str.split(",");
                if(name_list!=null && name_list.length>0){
                    rowsStr.addAll(Lists.newArrayList(name_list));
                }

                cur_rows = rowsStr.parallelStream().map(s->new DataPipe.Builder().udata(s).status(Const.FILE_STATUS_SUCCESS).task_type(strategyLogInfo.getInstance_type()).ext(new HashMap<>()).build()).collect(Collectors.toSet());

            }

            Set<DataPipe> rs=Sets.newHashSet();
            String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                    strategyLogInfo.getStrategy_group_instance_id());
            //解析上游任务并和当前节点数据做运算
            rs = calculateCommon(strategyLogInfo, "offline",cur_rows, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);

        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("label customlist run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

        }
    }

}
