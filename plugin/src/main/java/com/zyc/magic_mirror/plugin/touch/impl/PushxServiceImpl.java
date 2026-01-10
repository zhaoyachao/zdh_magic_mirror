package com.zyc.magic_mirror.plugin.touch.impl;

import com.google.common.collect.Lists;
import com.zyc.magic_mirror.common.util.ConfigUtil;
import com.zyc.magic_mirror.common.util.HttpUtil;
import com.zyc.magic_mirror.common.util.JsonUtil;
import com.zyc.magic_mirror.plugin.touch.PushxService;
import com.zyc.magic_mirror.plugin.touch.entity.BasePushTask;
import com.zyc.magic_mirror.plugin.touch.entity.PushxBaseResponse;
import com.zyc.magic_mirror.plugin.touch.entity.PushxTemplateResponse;

import java.util.List;
import java.util.Map;

/**
 * 消息推送服务
 */
public class PushxServiceImpl implements PushxService {

    private static final String SEND_URL = "/api/v1/pushx/message/send";

    @Override
    public PushxBaseResponse send(List<String> pushServers, String template_id, String account, String account_type, Map<String, Object> param) {
        try{
            BasePushTask basePushTask = new BasePushTask();
            basePushTask.setPush_servers(pushServers);
            basePushTask.setSource("zdh_magic_mirror");
            basePushTask.setTemplate_id(template_id);
            basePushTask.setAcc(account);
            basePushTask.setAcc_type(account_type);
            basePushTask.setParam(param);
            basePushTask.setTimestamp(System.currentTimeMillis() / 1000 + "");
            basePushTask.setSource_key(ConfigUtil.get(ConfigUtil.ZDH_PUSHX_SERVICE_KEY));

            basePushTask.setSign(basePushTask.getTimestamp()+basePushTask.getSource()+basePushTask.getSource_key());

            String request = JsonUtil.formatJsonString(basePushTask);
            String response = HttpUtil.builder().retryCount(1).postJSON(ConfigUtil.get(ConfigUtil.ZDH_PUSHX_BASE_URL) + SEND_URL, request);
            PushxBaseResponse pushxBaseResponse = JsonUtil.toJavaBean(response, PushxBaseResponse.class);
            return pushxBaseResponse;

        }catch (Exception e){
            return PushxBaseResponse.builder().code(-1).msg("推送失败: "+e.getMessage()).build();
        }
    }

    @Override
    public PushxTemplateResponse getTemplate(String templateId) {
        return null;
    }
}
