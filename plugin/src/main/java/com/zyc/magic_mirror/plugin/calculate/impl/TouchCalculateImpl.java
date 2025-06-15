package com.zyc.magic_mirror.plugin.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.entity.DataPipe;
import com.zyc.magic_mirror.common.entity.TouchConfigInfo;
import com.zyc.magic_mirror.common.util.Const;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.common.util.LogUtil;
import com.zyc.magic_mirror.plugin.calculate.CalculateResult;
import com.zyc.magic_mirror.plugin.impl.TouchServiceImpl;
import com.zyc.magic_mirror.plugin.touch.EmailTouch;
import com.zyc.magic_mirror.plugin.touch.SmsResponse;
import com.zyc.magic_mirror.plugin.touch.SmsTouch;
import com.zyc.magic_mirror.plugin.touch.impl.AliSmsTouch;
import com.zyc.magic_mirror.plugin.touch.impl.QQEmailTouch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 分流实现
 */
public class TouchCalculateImpl extends BaseCalculate implements Runnable {
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

    public TouchCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger, Properties dbConfig){
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
        TouchServiceImpl touchService=new TouchServiceImpl();
        String logStr="";
        try{

            //获取标签code
            String touch_task=run_jsmind_data.get("touch_task").toString();
            String touch_id=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用


            //生成参数
            CalculateResult calculateResult = calculateResult(strategyLogInfo, strategyLogInfo.getBase_path(), run_jsmind_data, param, strategyInstanceService);
            Set<DataPipe> rs = calculateResult.getRs();
            String file_dir = calculateResult.getFile_dir();



            if(is_disenable.equalsIgnoreCase("true")){
                //禁用,不做操作
            }else{
                TouchConfigInfo touchConfigInfo = touchService.selectById(touch_id);
                logStr = StrUtil.format("task: {}, touch_type: {}", strategyLogInfo.getStrategy_instance_id(), touch_task);
                LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
                if(touch_task.equalsIgnoreCase("email")){
                    rs = emailTouch(touchConfigInfo,rs, file_dir,strategyLogInfo.getStrategy_instance_id());
                }else if(touch_task.equalsIgnoreCase("sms")){
                    rs = smsTouch(touchConfigInfo,rs, file_dir,strategyLogInfo.getStrategy_instance_id());
                }
            }

            Set<DataPipe> rs3 = rs.parallelStream().filter(s->s.getStatus().equalsIgnoreCase(Const.FILE_STATUS_SUCCESS)).collect(Collectors.toSet());
            Set<DataPipe> rs_error = rs.parallelStream().filter(s->s.getStatus().equalsIgnoreCase(Const.FILE_STATUS_FAIL)).collect(Collectors.toSet());


            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo, rs3, rs_error);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs3, Const.STATUS_FINISH);
        }catch (Exception e){
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("plugin touch run error: ", e);
            writeEmptyFileAndStatus(strategyLogInfo);
        }finally {

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
    public Set<DataPipe> emailTouch(TouchConfigInfo touchConfigInfo,Set<DataPipe> rs,String file_dir, String id) throws Exception {
        EmailTouch emailTouch=new QQEmailTouch();
        emailTouch.init(new HashMap<String,Object>(this.dbConfig), touchConfigInfo);

        Set<DataPipe> tmp = Sets.newHashSet();

        Set<DataPipe> rs_tmp = Sets.newHashSet();
        List<String> ccs=new ArrayList<>();
        for (DataPipe email:rs){
            if(email.getUdata().contains("@")){
                rs_tmp.add(email);
                //writeFile(file_dir+"/email_"+id, Sets.newHashSet(email+",format error,"+System.currentTimeMillis()));
            }else{
                email.setStatus(Const.FILE_STATUS_FAIL);
                tmp.add(email);
            }
        }

        List<DataPipe> his = readHisotryFile(file_dir, "email_"+id, Const.FILE_STATUS_ALL);
        Set<String> hisSet = Sets.newHashSet(his.stream().map(s->s.getUdata()).collect(Collectors.toSet()));
        Set<DataPipe> diff = rs_tmp.parallelStream().filter(s->!hisSet.contains(s.getUdata())).collect(Collectors.toSet());

        for(DataPipe account: diff){
            String result = emailTouch.send(account.getUdata());
            if(result.equalsIgnoreCase("fail")){
                account.setStatus(Const.FILE_STATUS_FAIL);
            }

            tmp.add(account);
        }

        tmp.addAll(his);
        writeFile(file_dir+"/email_"+id, tmp);

        return tmp;
    }

    public Set<DataPipe> smsTouch(TouchConfigInfo touchConfigInfo,Set<DataPipe> rs,String file_dir, String id) throws Exception {
        List<List<DataPipe>> partitions = Lists.partition(Lists.newArrayList(rs) , 1000);
        //读取已经推送的信息
        List<DataPipe> his = readHisotryFile(file_dir, "sms_"+id, Const.FILE_STATUS_ALL);

        Set<String> hisSet = Sets.newHashSet(his.parallelStream().map(s->s.getUdata()).collect(Collectors.toSet()));

        Set<DataPipe> tmp = Sets.newHashSet();

        for (List<DataPipe> partition: partitions){
            Set<DataPipe> now = Sets.newHashSet(partition);

            Set<DataPipe> diff = now.parallelStream().filter(s->!hisSet.contains(s.getUdata())).collect(Collectors.toSet());
            Map<String, Object> jsonObject=JsonUtil.toJavaMap(touchConfigInfo.getTouch_config());
            String sign = touchConfigInfo.getSign();
            String template = touchConfigInfo.getTemplate_code();

            String platform = touchConfigInfo.getPlatform();
            SmsTouch smsTouch=getSmsTouchByPlatform(platform);
            String phones = StringUtils.join(diff,",");
            Properties properties = getSmsConfigByPlatform(platform);
            SmsResponse response = smsTouch.sendSms(properties, phones, sign, template,"","");

            for (DataPipe s: diff){
                if(!response.getCode().equalsIgnoreCase("ok")){
                    s.setStatus(Const.FILE_STATUS_FAIL);
                }
                Map<String, Object> stringObjectMap = JsonUtil.toJavaMap(s.getExt());
                stringObjectMap.put("touch_result", response.getObject());
                s.setExt(JsonUtil.formatJsonString(stringObjectMap));
                tmp.add(s);
            }
        }

        writeFile(file_dir+"/sms_"+id, tmp);

        return tmp;
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
