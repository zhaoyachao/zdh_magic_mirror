package com.zyc.magic_mirror.plugin.touch.entity;

import java.util.List;
import java.util.Map;

/**
 * 系统生成参数
 */
public class BaseSystemPushTask implements PushTask{

    /**
     * 由当前服务自动生成(表示当前一次会话id)
     */
    private long request_id;

    /**
     * 由当前服务生成,唯一的一条消息id
     */
    private long message_id;

    /**
     * 请求时间,毫秒级时间戳
     */
    private String request_time;

    /**
     * 当前推送服务
     */
    private String push_server;

    private String push_msg_type;

    /**
     * 队列名称
     */
    private String queue_name;

    /**
     * 通道池
     */
    private String channel_pool;

    /**
     * 增加此参数的原因:调用方并不能感知到真正的推送账号,只有一个临时token或者密钥,通道需要自行实现账号转换
     *
     * 通道可识别的用户账号
     */
    private String channel_acc;

    /**
     * 通道用户账号类型
     */
    private String channel_acc_type;

    /**
     * 设备类型, ios, huawei, android
     * 根据账号补充
     */
    private String device_type;

    /**
     * 设备ID
     */
    private String device_id;

    /**
     * app标识, 主要用于push推送时区分app应用
     */
    private String app;

    /**
     * huawei,xiaomi,meizu,oppo,vivo,honor,baiduandroid,jpush,getui
     */
    private String push_channel;

//    /**
//     * 通道商模版id
//     */
//    private String channel_template_id;

    /**
     * 当前url对应的服务必须由服务所属方提供
     *
     * 完整id mapping url
     * http://127.0.0.1/id_mapping?acc=xxx&acc_type=xx&acc_service=xx&&mapping_acc_type
     * 请求类型: post
     * 参数
     *   acc:当前账号
     *   acc_type: 当前账号类型
     *   acc_service: 当前账号对应服务
     *   mapping_acc_type: 转换的账号类型
     *
     *   source: 分配账号
     *   source_key: 分配密码
     */
    private String id_mapping_url;

    //******************push******************
    //push专用
    private Message message;
    //******************push******************

    //******************email******************
    /**
     * 是否html
     * true/false
     */
    private boolean html=false;

    public boolean isHtml() {
        return html;
    }

    public void setHtml(boolean html) {
        this.html = html;
    }
    //******************email*******************


    //private String url;


    //*************同步请求状态***********
    /**
     * 当前任务状态
     * 1:新建,2:发送中,3:调用成功,4:调用失败
     */
    private String status;
    /**
     * 失败码
     */
    private String error_code;
    /**
     * 失败说明
     */
    private String error_msg;


    //*************状态回调***********

    private String third_code;

    private String third_msg;

    private String deliver_time;

    private String deliver_status;

    //*************已经使用过的通道******************
    private List<String> channels;

    public long getRequest_id() {
        return request_id;
    }

    public void setRequest_id(long request_id) {
        this.request_id = request_id;
    }

    public long getMessage_id() {
        return message_id;
    }

    public void setMessage_id(long message_id) {
        this.message_id = message_id;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public String getPush_server() {
        return push_server;
    }

    public void setPush_server(String push_server) {
        this.push_server = push_server;
    }

    public String getPush_msg_type() {
        return push_msg_type;
    }

    public void setPush_msg_type(String push_msg_type) {
        this.push_msg_type = push_msg_type;
    }

    public String getQueue_name() {
        return queue_name;
    }


    public void setQueue_name(String queue_name) {
        this.queue_name = queue_name;
    }

    public String getChannel_pool() {
        return channel_pool;
    }

    public void setChannel_pool(String channel_pool) {
        this.channel_pool = channel_pool;
    }

    public String getChannel_acc() {
        return channel_acc;
    }

    public void setChannel_acc(String channel_acc) {
        this.channel_acc = channel_acc;
    }

    public String getChannel_acc_type() {
        return channel_acc_type;
    }

    public void setChannel_acc_type(String channel_acc_type) {
        this.channel_acc_type = channel_acc_type;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

//    public String getChannel_template_id() {
//        return channel_template_id;
//    }
//
//    public void setChannel_template_id(String channel_template_id) {
//        this.channel_template_id = channel_template_id;
//    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getPush_channel() {
        return push_channel;
    }

    public void setPush_channel(String push_channel) {
        this.push_channel = push_channel;
    }

    public String getId_mapping_url() {
        return id_mapping_url;
    }

    public void setId_mapping_url(String id_mapping_url) {
        this.id_mapping_url = id_mapping_url;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }


//    public String getApp_id() {
//        return app_id;
//    }
//
//    public void setApp_id(String app_id) {
//        this.app_id = app_id;
//    }

//    public String getMiniprogram_state() {
//        return miniprogram_state;
//    }
//
//    public void setMiniprogram_state(String miniprogram_state) {
//        this.miniprogram_state = miniprogram_state;
//    }

//    public String getPage() {
//        return page;
//    }
//
//    public void setPage(String page) {
//        this.page = page;
//    }

//    public String getLang() {
//        return lang;
//    }
//
//    public void setLang(String lang) {
//        this.lang = lang;
//    }

//    public String getUrl() {
//        return url;
//    }
//
//    public void setUrl(String url) {
//        this.url = url;
//    }


    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError_code() {
        return error_code;
    }

    public void setError_code(String error_code) {
        this.error_code = error_code;
    }

    public String getError_msg() {
        return error_msg;
    }

    public void setError_msg(String error_msg) {
        this.error_msg = error_msg;
    }

    public String getThird_code() {
        return third_code;
    }

    public void setThird_code(String third_code) {
        this.third_code = third_code;
    }

    public String getThird_msg() {
        return third_msg;
    }

    public void setThird_msg(String third_msg) {
        this.third_msg = third_msg;
    }

    public String getDeliver_time() {
        return deliver_time;
    }

    public void setDeliver_time(String deliver_time) {
        this.deliver_time = deliver_time;
    }

    public String getDeliver_status() {
        return deliver_status;
    }

    public void setDeliver_status(String deliver_status) {
        this.deliver_status = deliver_status;
    }

    public List<String> getChannels() {
        return channels;
    }

    public void setChannels(List<String> channels) {
        this.channels = channels;
    }

    public static class Message{

        /**
         * 场景,用于区分消息
         */
        private String scene;
        /**
         * 声音
         */
        private String sound;

        /**
         * 图标
         */
        private String icon;

        /**
         * 图标颜色
         */
        private String color;

        /**
         * 过期时间
         */
        private String ttl;

        /**
         * 覆盖上次推送
         */
        private String overwrite_msg;

        /**
         * 点击类型
         * 0:无操作,1:打开链接,2:跳转app,3:打开媒体
         * 后续可增加
         */
        private String click_type;

        /**
         * 点击打开的url,打开媒体时也可使用当前参数
         */
        private String click_url;

        /**
         * 点击跳转的app
         */
        private String click_app;

        /**
         * 自定义参数
         */
        private Map<String, Object> customer_param;

        public String getScene() {
            return scene;
        }

        public void setScene(String scene) {
            this.scene = scene;
        }

        public String getSound() {
            return sound;
        }

        public void setSound(String sound) {
            this.sound = sound;
        }

        public String getIcon() {
            return icon;
        }

        public void setIcon(String icon) {
            this.icon = icon;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public String getTtl() {
            return ttl;
        }

        public void setTtl(String ttl) {
            this.ttl = ttl;
        }

        public String getOverwrite_msg() {
            return overwrite_msg;
        }

        public void setOverwrite_msg(String overwrite_msg) {
            this.overwrite_msg = overwrite_msg;
        }

        public String getClick_type() {
            return click_type;
        }

        public void setClick_type(String click_type) {
            this.click_type = click_type;
        }

        public String getClick_url() {
            return click_url;
        }

        public void setClick_url(String click_url) {
            this.click_url = click_url;
        }

        public String getClick_app() {
            return click_app;
        }

        public void setClick_app(String click_app) {
            this.click_app = click_app;
        }

        public Map<String, Object> getCustomer_param() {
            return customer_param;
        }

        public void setCustomer_param(Map<String, Object> customer_param) {
            this.customer_param = customer_param;
        }
    }
}
