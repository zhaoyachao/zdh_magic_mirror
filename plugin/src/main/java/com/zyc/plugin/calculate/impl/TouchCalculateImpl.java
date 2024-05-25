package com.zyc.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.entity.TouchConfigInfo;
import com.zyc.common.util.Const;
import com.zyc.common.util.LogUtil;
import com.zyc.plugin.calculate.CalculateResult;
import com.zyc.plugin.calculate.TouchCalculate;
import com.zyc.plugin.impl.StrategyInstanceServiceImpl;
import com.zyc.plugin.impl.TouchServiceImpl;
import com.zyc.plugin.touch.EmailTouch;
import com.zyc.plugin.touch.SmsResponse;
import com.zyc.plugin.touch.SmsTouch;
import com.zyc.plugin.touch.impl.AliSmsTouch;
import com.zyc.plugin.touch.impl.QQEmailTouch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分流实现
 */
public class TouchCalculateImpl extends BaseCalculate implements TouchCalculate {
    private static Logger logger= LoggerFactory.getLogger(TouchCalculateImpl.class);

    /**
     {
     "strategy_instance": [
     {
     "id" : 1036227672688037890,
     "strategy_context" : "cessss",
     "group_id" : "1033156620940480512",
     "group_context" : "测试第一个标签策略",
     "group_instance_id" : "1036227672352493568",
     "instance_type" : "shunt",
     "start_time" : "2022-10-30 10:38:47",
     "end_time" : null,
     "jsmind_data" : "{\"positionY\":461,\"shunt_context\":\"cessss\",\"shunt\":\"9bd_140_a7bc_ea\",\"touch_type\":\"database\",\"shunt_param\":\"[{\\\"shunt_code\\\":\\\"A\\\",\\\"shunt_name\\\":\\\"分流3个\\\",\\\"shunt_type\\\":\\\"in\\\",\\\"shunt_value\\\":\\\"3\\\"}]\",\"name\":\"cessss\",\"more_task\":\"shunt\",\"id\":\"c42_8c8_8b1a_2a\",\"type\":\"shunt\",\"divId\":\"c42_8c8_8b1a_2a\",\"time_out\":\"86400\",\"positionX\":311}",
     "owner" : "zyc",
     "is_delete" : "0",
     "create_time" : "2022-10-21 23:15:34",
     "update_time" : "2022-10-30 10:38:49",
     "expr" : null,
     "misfire" : "0",
     "priority" : "",
     "status" : "create",
     "quartz_time" : null,
     "use_quartz_time" : null,
     "time_diff" : null,
     "schedule_source" : "2",
     "cur_time" : "2022-10-30 10:38:47",
     "run_time" : "2022-10-30 10:38:49",
     "run_jsmind_data" : "{\"positionY\":461,\"shunt_context\":\"cessss\",\"shunt\":\"9bd_140_a7bc_ea\",\"touch_type\":\"database\",\"shunt_param\":\"[{\\\"shunt_code\\\":\\\"A\\\",\\\"shunt_name\\\":\\\"分流3个\\\",\\\"shunt_type\\\":\\\"in\\\",\\\"shunt_value\\\":\\\"3\\\"}]\",\"name\":\"cessss\",\"more_task\":\"shunt\",\"id\":\"c42_8c8_8b1a_2a\",\"type\":\"shunt\",\"divId\":\"c42_8c8_8b1a_2a\",\"time_out\":\"86400\",\"positionX\":311}",
     "next_tasks" : "",
     "pre_tasks" : "1036227672688037889",
     "is_disenable" : "false",
     "depend_level" : "0",
     "touch_type" : "database",
     "strategy_id" : "c42_8c8_8b1a_2a"
     }
     ]}
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public TouchCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
    public String getOperate(Map run_jsmind_data) {
        return run_jsmind_data.getOrDefault("operate", "or").toString();
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        TouchServiceImpl touchService=new TouchServiceImpl();
        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String touch_task=run_jsmind_data.get("touch_task").toString();
            String touch_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<String> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();



            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                TouchConfigInfo touchConfigInfo = touchService.selectById(touch_id);
                logStr = StrUtil.format("task: {}, touch_type: {}", strategyLogInfo.getStrategy_instance_id(), touch_task);
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                if(touch_task.equalsIgnoreCase("email")){
                    emailTouch(touchConfigInfo,rs, file_dir,strategyLogInfo.getStrategy_instance_id());
                }else if(touch_task.equalsIgnoreCase("sms")){
                    smsTouch(touchConfigInfo,rs, file_dir,strategyLogInfo.getStrategy_instance_id());
                }
            }

            Set<String> rs_error = Sets.difference(calculateResult.getRs(), rs);
            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);
        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin touch run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }

    /**
     * 邮件推送
     * @param touchConfigInfo
     * @param rs
     * @param file_dir
     * @param id
     * @throws IOException
     */
    public void emailTouch(TouchConfigInfo touchConfigInfo,Set<String> rs,String file_dir, String id) throws Exception {
        EmailTouch emailTouch=new QQEmailTouch();
        emailTouch.init(new HashMap<String,Object>(this.dbConfig), touchConfigInfo);
        List<String> ccs=new ArrayList<>();
        for (String email:rs){
            if(!email.contains("@")){
                rs.remove(email);
                writeFile(file_dir+"/email_"+id, Sets.newHashSet(email+",format error,"+System.currentTimeMillis()));
            }
        }

        List<String> his = readHisotryFile(file_dir, "email_"+id, Const.FILE_STATUS_ALL);
        Set<String> hisSet = Sets.newHashSet(his);
        Set<String> diff = Sets.difference(rs, hisSet);
        Set<String> tmp = Sets.newHashSet();
        for(String account: diff){
            String result = emailTouch.send(account);
            tmp.add(account+","+result+","+System.currentTimeMillis());
            writeFile(file_dir+"/email_"+id, tmp);
        }
    }

    public void smsTouch(TouchConfigInfo touchConfigInfo,Set<String> rs,String file_dir, String id) throws Exception {
        List<List<String>> partitions = Lists.partition(new ArrayList<>(rs) , 1000);
        //读取已经推送的信息
        List<String> his = readHisotryFile(file_dir, "sms_"+id, Const.FILE_STATUS_ALL);
        Set<String> hisSet = Sets.newHashSet(his);
        for (List<String> partition: partitions){
            Set<String> now = Sets.newHashSet(partition);
            Set<String> diff = Sets.difference(now, hisSet);
            JSONObject jsonObject=JSONObject.parseObject(touchConfigInfo.getTouch_config());
            String sign = touchConfigInfo.getSign();
            String template = touchConfigInfo.getTemplate_code();

            String platform = touchConfigInfo.getPlatform();
            SmsTouch smsTouch=getSmsTouchByPlatform(platform);
            String phones = StringUtils.join(diff,",");
            Properties properties = getSmsConfigByPlatform(platform);
            SmsResponse response = smsTouch.sendSms(properties, phones, sign, template,"","");
            Set<String> tmp = Sets.newHashSet();
            for (String s: diff){
                tmp.add(s+","+response.getCode()+","+response.getMessage()+","+JSON.toJSONString(response.getObject())+","+System.currentTimeMillis());
            }
            writeFile(file_dir+"/sms_"+id, tmp);
        }
    }

    public SmsTouch getSmsTouchByPlatform(String platform) throws Exception {
        if(platform.equalsIgnoreCase("ali")){
            return new AliSmsTouch();
        }
        try {
            return (SmsTouch) Class.forName(platform).newInstance();
        } catch (InstantiationException e) {
            logger.error("plugin touch load platform error: ", e);
        } catch (IllegalAccessException e) {
            logger.error("plugin touch load platform error: ", e);
        } catch (ClassNotFoundException e) {
            logger.error("plugin touch load platform error: ", e);
        }

        throw new Exception("无法找到适配的短信服务,请检查短信平台参数配置是否正常");
    }

    public Properties getSmsConfigByPlatform(String platform) throws Exception {
        if(platform.equalsIgnoreCase("ali")){
            Properties properties = new Properties();
            properties.putAll(this.dbConfig);
            return properties;
        }
        try {
            return  new Properties();
        } catch (Exception e) {
            logger.error("plugin touch run error: ", e);
        }

        throw new Exception("无法找到适配的短信服务,请检查短信平台参数配置是否正常");
    }

}
