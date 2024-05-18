package com.zyc.plugin.touch.impl;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsRequest;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.profile.DefaultProfile;
import com.zyc.plugin.touch.SmsResponse;
import com.zyc.plugin.touch.SmsTouch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AliSmsTouch implements SmsTouch {
    private static Logger logger= LoggerFactory.getLogger(AliSmsTouch.class);
    @Override
    public SmsResponse sendSms(Properties config, String phones, String sigin, String template, String param, String outId) throws Exception {
        SmsResponse smsResponse=new SmsResponse();
        DefaultProfile profile = DefaultProfile.getProfile(config.getProperty("regin.id"), config.getProperty("ak"), config.getProperty("sk"));
        IAcsClient client = new DefaultAcsClient(profile);

        // 创建API请求并设置参数
        SendSmsRequest request = new SendSmsRequest();
        request.setPhoneNumbers(phones);
        request.setSignName(sigin);
        request.setTemplateCode(template);
        request.setTemplateParam(param);
        request.setOutId(outId);
        try {
            SendSmsResponse response = client.getAcsResponse(request);
            smsResponse.setCode(response.getCode());
            smsResponse.setMessage(response.getMessage());
            smsResponse.setObject(response);
            return smsResponse;
        } catch (ServerException e) {
            logger.error("plugin touch alisms error: ", e);
            throw e;
        } catch (ClientException e) {
            // 打印错误码
            System.out.println("ErrCode:" + e.getErrCode());
            System.out.println("ErrMsg:" + e.getErrMsg());
            System.out.println("RequestId:" + e.getRequestId());
            throw e;
        }

    }
}
