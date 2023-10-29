package com.zyc.plugin.calculate.impl;

import cn.hutool.extra.mail.MailAccount;
import cn.hutool.extra.mail.MailUtil;
import org.junit.Test;

import java.util.Arrays;

public class TouchCalculateImplTest {
    @Test
    public void run() {

        MailAccount account = new MailAccount();
        account.setHost("smtp.qq.com");//邮件服务器的SMTP地址，可选，默认为smtp.<发件人邮箱后缀>
        account.setPort(587);//邮件服务器的SMTP端口
        account.setAuth(true);
        account.setFrom("1209687056@qq.com");//发件人（必须正确，否则发送失败）
        account.setUser("1209687056@qq.com");//用户名，默认为发件人邮箱前缀
        account.setPass("");//前面开启POP3/SMTP服务拿到的密码
        account.setStarttlsEnable(true);
        account.setSslEnable(false);

        MailUtil.send(account, Arrays.asList(new String[]{"1299898281@qq.com","1209687056@qq.com"}), "ZDH测试邮件","ZDH测试邮件",false);

    }
}
