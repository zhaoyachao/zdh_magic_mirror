package com.zyc.common.entity;

import com.zyc.common.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.List;

public class PluginInfo {

    private String id;

    /**
     * 插件类型
     */
    private String plugin_type;

    /**
     * 插件code
     */
    private String plugin_code;

    /**
     * 插件名称
     */
    private String plugin_name;

    /**
     * 插件配置
     */
    private String plugin_json;

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
     * 是否删除,0:未删除,1:删除
     */
    private String is_delete;

    private List<Object> param_json_object;

    public  List<Object> getParam_json_object() {
        if(!StringUtils.isEmpty(plugin_json)){
            return JsonUtil.toJavaList(plugin_json);
        }
        return param_json_object;
    }

    /**
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    public String getPlugin_type() {
        return plugin_type;
    }

    public void setPlugin_type(String plugin_type) {
        this.plugin_type = plugin_type;
    }

    /**
     * 获取插件code
     *
     * @return plugin_code - 插件code
     */
    public String getPlugin_code() {
        return plugin_code;
    }

    /**
     * 设置插件code
     *
     * @param plugin_code 插件code
     */
    public void setPlugin_code(String plugin_code) {
        this.plugin_code = plugin_code;
    }

    /**
     * 获取插件名称
     *
     * @return plugin_name - 插件名称
     */
    public String getPlugin_name() {
        return plugin_name;
    }

    /**
     * 设置插件名称
     *
     * @param plugin_name 插件名称
     */
    public void setPlugin_name(String plugin_name) {
        this.plugin_name = plugin_name;
    }

    /**
     * 获取插件配置
     *
     * @return plugin_json - 插件配置
     */
    public String getPlugin_json() {
        return plugin_json;
    }

    /**
     * 设置插件配置
     *
     * @param plugin_json 插件配置
     */
    public void setPlugin_json(String plugin_json) {
        this.plugin_json = plugin_json;
    }

    /**
     * 获取拥有者
     *
     * @return owner - 拥有者
     */
    public String getOwner() {
        return owner;
    }

    /**
     * 设置拥有者
     *
     * @param owner 拥有者
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * 获取创建时间
     *
     * @return create_time - 创建时间
     */
    public Timestamp getCreate_time() {
        return create_time;
    }

    /**
     * 设置创建时间
     *
     * @param create_time 创建时间
     */
    public void setCreate_time(Timestamp create_time) {
        this.create_time = create_time;
    }

    /**
     * 获取更新时间
     *
     * @return update_time - 更新时间
     */
    public Timestamp getUpdate_time() {
        return update_time;
    }

    /**
     * 设置更新时间
     *
     * @param update_time 更新时间
     */
    public void setUpdate_time(Timestamp update_time) {
        this.update_time = update_time;
    }

    /**
     * 获取是否删除,0:未删除,1:删除
     *
     * @return is_delete - 是否删除,0:未删除,1:删除
     */
    public String getIs_delete() {
        return is_delete;
    }

    /**
     * 设置是否删除,0:未删除,1:删除
     *
     * @param is_delete 是否删除,0:未删除,1:删除
     */
    public void setIs_delete(String is_delete) {
        this.is_delete = is_delete;
    }
}