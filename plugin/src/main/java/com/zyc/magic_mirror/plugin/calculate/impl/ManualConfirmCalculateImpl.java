package com.zyc.magic_mirror.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.NoticeInfo;
import com.zyc.magic_mirror.common.entity.PermissionUserInfo;
import com.zyc.magic_mirror.common.service.impl.NoticeServiceImpl;
import com.zyc.magic_mirror.common.service.impl.PermissionUserServiceImpl;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 人工确认
 */
public class ManualConfirmCalculateImpl extends BaseCalculate {
    private static Logger logger= LoggerFactory.getLogger(ManualConfirmCalculateImpl.class);

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

    public ManualConfirmCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JsonUtil.toJavaBean(this.param.get("run_jsmind_data").toString(), Map.class);
            String[] confirm_notice_types = run_jsmind_data.getOrDefault("confirm_notice_type","").toString().split(",");
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            String product_code=dbConfig.get("product.code");
            String zdh_web_url=dbConfig.get("zdh_web_url");



            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();
            boolean check = false;
            if(is_disenable.equalsIgnoreCase("true")){
                check=true;
            }else{
                //读取历史done文件确认是否执行过
                List<String> historys = readFile(strategyLogInfo.getFile_path()+"_done");

                if(historys == null || historys.size() == 0){
                    //通知用户
                    String owner = this.param.getOrDefault("owner", "").toString();
                    if(!StringUtils.isEmpty(owner) && confirm_notice_types.length > 0){
                        //根据账号查询,邮件,短信
                        NoticeInfo noticeInfo = new NoticeInfo();
                        noticeInfo.setMsg_type("通知");
                        noticeInfo.setMsg_title("策略执行流程确认");
                        noticeInfo.setMsg("策略组: "+strategyLogInfo.getStrategy_group_id()+", 策略组实例: "+strategyLogInfo.getStrategy_group_instance_id()+", 策略实例"+strategyLogInfo.getStrategy_instance_id()+",需要手动确认,操作路径智能营销>>智能策略>>执行记录>>子任务>>跳过, 快速跳转地址: "+zdh_web_url+"/strategy_instance_index.html?strategy_group_instance_id="+strategyLogInfo.getStrategy_group_instance_id());
                        noticeInfo.setMsg_url("");
                        noticeInfo.setIs_see("false");
                        noticeInfo.setOwner(owner);

                        for (String confirm_notice_type: confirm_notice_types){
                            String account = getAccount(owner, confirm_notice_type, product_code);
                            boolean is_success = send(account, confirm_notice_type, noticeInfo);
                            if(is_success) {
                                check=true;
                            }
                            logStr = StrUtil.format("task: {}, notice {} {}, owner: {}", strategyLogInfo.getStrategy_instance_id(), confirm_notice_type, is_success, owner);
                            LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                        }
                    }
                    //写入标识文件,标记当前任务执行过
                    writeFile(strategyLogInfo.getFile_path()+"_done", Sets.newHashSet(new DataPipe.Builder().udata("done").build()));
                }else{
                    logStr = StrUtil.format("task: {}, alerady send success", strategyLogInfo.getStrategy_instance_id());
                    LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                    check=true;
                }

            }

            if(!check){
                throw new Exception("send notice error");
            }

            if(is_disenable.equalsIgnoreCase("true")){

                writeFileAndPrintLog(strategyLogInfo, rs);
                setStatus(strategyLogInfo.getStrategy_instance_id(), Const.STATUS_FINISH);
                logStr = StrUtil.format("task: {}, update status finish", strategyLogInfo.getStrategy_instance_id());
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
            }
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin manual confirm run error: ", e);
        }finally {

        }
    }

    private String getAccount(String owner, String confirm_notice_type, String product_code){
        if(confirm_notice_type.equalsIgnoreCase("email")){
            return getEmailByUserAccount(owner, product_code);
        }else if(confirm_notice_type.equalsIgnoreCase("sms")){
            return getSmsByUserAccount(owner, product_code);
        }else if(confirm_notice_type.equalsIgnoreCase("zdh")){
            return owner;
        }
        return "";
    }

    private String getEmailByUserAccount(String owner, String product_code){
        PermissionUserServiceImpl permissionUserService = new PermissionUserServiceImpl();
        PermissionUserInfo permissionUserInfo = permissionUserService.selectUserInfo(owner, product_code);
        if(permissionUserInfo != null && permissionUserInfo.getEmail() != null){
            return permissionUserInfo.getEmail();
        }
        return "";
    }

    private String getSmsByUserAccount(String owner, String product_code){
        PermissionUserServiceImpl permissionUserService = new PermissionUserServiceImpl();
        PermissionUserInfo permissionUserInfo = permissionUserService.selectUserInfo(owner, product_code);
        if(permissionUserInfo != null && permissionUserInfo.getPhone() != null){
            return permissionUserInfo.getPhone();
        }
        return "";
    }

    private boolean send(String account, String confirm_notice_type, NoticeInfo noticeInfo){
        try{
            if(confirm_notice_type.equalsIgnoreCase("email")){
                return false;
            }else if(confirm_notice_type.equalsIgnoreCase("sms")){
                return false;
            }else if(confirm_notice_type.equalsIgnoreCase("zdh")){
                return sendZdh(account, noticeInfo);
            }
        }catch (Exception e){
            String logStr = StrUtil.format("task: {}, send error: {}", this.strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            logger.info(logStr);
            LogUtil.error("",this.strategyLogInfo.getStrategy_instance_id(), logStr);
        }
        return false;
    }

    private boolean sendZdh(String account, NoticeInfo noticeInfo){
        try{
            NoticeServiceImpl noticeService=new NoticeServiceImpl();
            noticeInfo.setOwner(account);
            noticeService.send(noticeInfo);
            return true;
        }catch (Exception e){
            String logStr = StrUtil.format("task: {}, send zdh error: {}", this.strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            logger.info(logStr);
            LogUtil.error("",this.strategyLogInfo.getStrategy_instance_id(), logStr);
        }

        return false;
    }


}
