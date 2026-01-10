package com.zyc.magic_mirror.plugin.touch.entity;

import java.util.List;
import java.util.Map;

/**
 * 请求最少参数
 */
public class BasePushTask extends BaseSystemPushTask{

    private String timestamp;
    private String source_key;
    private String sign;
    /**
     * 来源
     * 一般指平台名称
     */
    private String source;

    /**
     * 子来源
     * 一般在saas服务中使用,比如都来自zdh平台, zdh是个saas平台, 这里sub_source 可以传递saas服务使用方公司的标识
     */
    private String sub_source;

    /**
     * 推送服务类型
     * email:邮箱, sms:短信, app:apppush, applet:小程序, official_account:公众号, feishu: 飞书, dingding:丁丁
     */
    private List<String> push_servers;

    /**
     * 推送模版id
     */
    private String template_id;

    /**
     * 推送账号
     */
    private String acc;

    /**
     * 账号类型
     */
    private String acc_type;

//    /**
//     * 推送标题
//     */
//    private String title;

//    /**
//     * 推送内容，支持变量
//     * 当前结构可以是一个json,具体和每个触达方式有关
//     */
//    private String content;

//    /**
//     * 内容签名
//     */
//    private String sign_name;

    /**
     * 自定义参数
     * 前缀定义push_{push_server}_{参数名}
     * 需要注意参数的顺序,因部分通道按顺序进行参数划分
     * json结构{"push_sms_k1": "v1", "push_sms_k2": "v2"}
     */
    private Map<String, Object> param;

    /**
     * 其他
     */
    private String ref;

    private String channel;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getSource_key() {
        return source_key;
    }

    public void setSource_key(String source_key) {
        this.source_key = source_key;
    }

    public String getSign() {
        return sign;
    }
    public void setSign(String sign) {
        this.sign = sign;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSub_source() {
        return sub_source;
    }

    public void setSub_source(String sub_source) {
        this.sub_source = sub_source;
    }

    public List<String> getPush_servers() {
        return push_servers;
    }

    public void setPush_servers(List<String> push_servers) {
        this.push_servers = push_servers;
    }

    public String getTemplate_id() {
        return template_id;
    }

    public void setTemplate_id(String template_id) {
        this.template_id = template_id;
    }

    public String getAcc() {
        return acc;
    }

    public void setAcc(String acc) {
        this.acc = acc;
    }

    public String getAcc_type() {
        return acc_type;
    }

    public void setAcc_type(String acc_type) {
        this.acc_type = acc_type;
    }

//    public String getTitle() {
//        return title;
//    }
//
//    public void setTitle(String title) {
//        this.title = title;
//    }

//    public String getContent() {
//        return content;
//    }
//
//    public void setContent(String content) {
//        this.content = content;
//    }

//    public String getSign_name() {
//        return sign_name;
//    }
//
//    public void setSign_name(String sign_name) {
//        this.sign_name = sign_name;
//    }

    public Map<String, Object> getParam() {
        return param;
    }

    public void setParam(Map<String, Object> param) {
        this.param = param;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }
}
