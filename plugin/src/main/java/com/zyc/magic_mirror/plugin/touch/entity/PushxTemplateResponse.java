package com.zyc.magic_mirror.plugin.touch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Transient;
import java.sql.Timestamp;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PushxTemplateResponse extends PushxBaseResponse{
    private TemplateData template;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TemplateData {
        private String id;

        /**
         * 模板名称
         */
        private String template_name;

        /**
         * 模板Id
         */
        private String template_id;

        /**
         * 消息类型,1:营销,2:通知,3:验证码,4:告警,5:其他
         */
        private String push_type;

        /**
         * 消息类型,1:营销,2:通知,3:验证码,4:告警,5:其他
         */
        private String push_msg_type;

        /**
         * 推送服务,sms,email
         */
        private String push_server;

        /**
         * 状态,0:编辑,1:启用,2:审批中,3:审批失败,4:禁用
         */
        private String status;

        /**
         * 拥有者
         */
        private String owner;

        /**
         * 创建时间
         */
        private Timestamp create_time;

        /**
         * 更新时间
         */
        private Timestamp update_time;

        /**
         * 更新说明
         */
        private String update_context;

        /**
         * 是否删除,0:未删除,1:删除
         */
        private String is_delete;

        /**
         * 产品code
         */
        private String product_code;

        /**
         * 用户组
         */
        private String dim_group;

        /**
         * 推送配置,json结构
         */
        private String config;

        @Transient
        private Map<String,Object> configMap;
    }
}
