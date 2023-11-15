package com.zyc.plugin.touch.impl;

import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import cn.hutool.extra.template.Template;
import cn.hutool.extra.template.TemplateConfig;
import cn.hutool.extra.template.TemplateEngine;
import cn.hutool.extra.template.TemplateUtil;
import com.alibaba.fastjson.JSON;
import com.zyc.common.entity.TouchConfigInfo;
import com.zyc.plugin.touch.EmailTouch;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QQEmailTouch implements EmailTouch {

    private TouchConfigInfo touchConfigInfo;
    private MailAccount mailAccount;

    @Override
    public void init(Map<String, Object> param, TouchConfigInfo touchConfigInfo) {
        this.touchConfigInfo = touchConfigInfo;
        MailAccount account = new MailAccount();

        Map<String, Object> mail = new HashMap<>();
        if(param != null){
            for (String key: param.keySet()){
                if(key.startsWith("zdh.email.")){
                    mail.put(key.replaceFirst("zdh.email.",""), param.get(key));
                }
            }
        }
        String mailStr = JSON.toJSONString(mail);
        account = JSON.parseObject(mailStr, MailAccount.class);
        this.mailAccount = account;
    }

    @Override
    public String send(String account) {
        try{
            String result = "fail";
            List<String> ccs=new ArrayList<>();
            //判断是否有动态参数,无动态参数,可批量发送-提高性能,有动态参数,只能多线程单个发送
            if(StringUtils.isEmpty(touchConfigInfo.getTouch_param_codes())){
                result = MailUtil.send(this.mailAccount,account, touchConfigInfo.getTouch_context(),touchConfigInfo.getTouch_config(),false);
            }else{
                Map<String,String> dynamicParams = getDynamicParamByUser(account,touchConfigInfo.getTouch_param_codes());
                boolean isSkip = false;
                //校验动态参数是否正常
                for(String param: touchConfigInfo.getTouch_param_codes().split(",")){
                    if(dynamicParams==null || dynamicParams.containsKey(param) || StringUtils.isEmpty(dynamicParams.get(param))){
                        isSkip = true;
                        throw new Exception("当前模块配置有动态参数,未拉取到参数信息,参数code: "+param);
                    }
                }

                if(!isSkip){
                    //引入模板
                    TemplateEngine engine = TemplateUtil.createEngine(new TemplateConfig());
                    Template template = engine.getTemplate(touchConfigInfo.getTouch_config());
                    String str = template.render(dynamicParams);
                    result = MailUtil.send(this.mailAccount,account, touchConfigInfo.getTouch_context(),str,false);
                }
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }
        return "fail";
    }


    /**
     * 本期先不使用参数,后期可实现动态参数
     * @param account
     * @param touchParamCodes
     * @return
     */
    @Override
    public Map<String, String> getDynamicParamByUser(String account, String touchParamCodes) {
        if(StringUtils.isEmpty(touchParamCodes)){
            return new HashMap<String,String>();
        }
        return null;
    }


}
