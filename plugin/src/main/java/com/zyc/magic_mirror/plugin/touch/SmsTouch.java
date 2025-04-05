package com.zyc.magic_mirror.plugin.touch;

import java.util.Properties;

public interface SmsTouch {
    public SmsResponse sendSms(Properties config, String phones, String sigin, String template, String param, String outId) throws Exception ;
}
