package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.NoticeInfo;
import com.zyc.common.entity.PermissionUserInfo;
import com.zyc.common.service.impl.NoticeServiceImpl;
import com.zyc.common.service.impl.PermissionUserServiceImpl;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.ManualConfirmCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 人工确认
 */
public class ManualConfirmCalculateImpl extends BaseCalculate implements ManualConfirmCalculate {
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
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public ManualConfirmCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
    }

    @Override
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
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
        String file_path="";
        try{

            localVar.set(id);
            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String[] confirm_notice_types = run_jsmind_data.getOrDefault("confirm_notice_type","").toString().split(",");
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用
            //调度逻辑时间,毫秒时间戳
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }
            String base_path=dbConfig.get("file.path");
            String product_code=dbConfig.get("product.code");

            file_path=getFilePath(base_path,group_id,group_instance_id,id);

            CalculateResult calculateResult = calculateResult(base_path, run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();
            boolean check = false;
            if(is_disenable.equalsIgnoreCase("true")){
                check=true;
            }else{
                //通知用户
                String owner = this.param.getOrDefault("owner", "").toString();
                if(!StringUtils.isEmpty(owner) && confirm_notice_types.length > 0){
                    //根据账号查询,邮件,短信
                    NoticeInfo noticeInfo = new NoticeInfo();
                    noticeInfo.setMsg_type("通知");
                    noticeInfo.setMsg_title("策略执行流程确认");
                    noticeInfo.setMsg("策略实例"+id+",需要手动确认,操作路径智能营销>>智能策略>>执行记录>>子任务>>跳过");
                    noticeInfo.setMsg_url("");
                    noticeInfo.setIs_see("false");
                    noticeInfo.setOwner(owner);

                    for (String confirm_notice_type: confirm_notice_types){
                        String account = getAccount(owner, confirm_notice_type, product_code);
                        boolean is_success = send(account, confirm_notice_type, noticeInfo);
                        if(is_success) check=true;
                        logStr = StrUtil.format("task: {}, notice {} {}, owner: {}", id, confirm_notice_type, is_success, owner);
                        LogUtil.info(strategy_id, id, logStr);
                    }
                }
            }

            if(!check){
                throw new Exception("send notice error");
            }

            String save_path = writeFile(id,file_path, rs);
            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(strategy_id, id, logStr);
            if(is_disenable.equalsIgnoreCase("true")){
                setStatus(id, Const.STATUS_FINISH);
                logStr = StrUtil.format("task: {}, update status finish", id);
                LogUtil.info(strategy_id, id, logStr);
            }
        }catch (Exception e){
            writeEmptyFile(file_path);
            setStatus(id, Const.STATUS_ERROR);
            LogUtil.error(strategy_id, id, e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
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
            String logStr = StrUtil.format("task: {}, send error: {}", localVar.get(), e.getMessage());
            logger.info(logStr);
            LogUtil.error("",localVar.get(), logStr);
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
            String logStr = StrUtil.format("task: {}, send zdh error: {}", localVar.get(), e.getMessage());
            logger.info(logStr);
            LogUtil.error("",localVar.get(), logStr);
        }

        return false;
    }


}
