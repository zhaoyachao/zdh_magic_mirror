package com.zyc.ship.entity;

import com.google.common.collect.Lists;
import com.zyc.ship.common.Const;
import org.apache.commons.lang3.StringUtils;

public class ShipCommonInputParam implements InputParam{

    /**
     * 产品code
     */
    private String product_code;

    /**
     * 用户id
     */
    private String uid;

    /**
     * id类型
     */
    private String id_type;

    /**
     * 数据来源-指数据系统
     */
    private String source;

    /**
     * 应用场景
     * online_manager: 在线经营
     * online_risk: 实时风控决策
     *
     */
    private String scene;

    /**
     * 数据类型-在经营策略中必须提前配置此信信息
     *
     * 代表本次请求,只从相同data_node的策略中做决策
     *
     * 此字段也可以代表事件的含义
     */
    private String data_node;

    /**
     * 参数,json 字符串
     *
     * {
     *     "tag_user_age": {"age": "20"}
     * }
     */
    private String param;


    public String getProduct_code() {
        return product_code;
    }

    public void setProduct_code(String product_code) {
        this.product_code = product_code;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getId_type() {
        return id_type;
    }

    public void setId_type(String id_type) {
        this.id_type = id_type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    public String getScene() {
        return scene;
    }

    public void setScene(String scene) {
        this.scene = scene;
    }

    public String getData_node() {
        return data_node;
    }

    public void setData_node(String data_node) {
        this.data_node = data_node;
    }

    @Override
    public void checkParams() throws Exception {

        if(StringUtils.isEmpty(product_code)){
            throw new Exception("product_code参数不可为空");
        }
        if(StringUtils.isEmpty(uid)){
            throw new Exception("uid参数不可为空");
        }
        if(StringUtils.isEmpty(id_type)){
            throw new Exception("id_type参数不可为空");
        }
        if(StringUtils.isEmpty(source)){
            throw new Exception("source参数不可为空");
        }
        if(StringUtils.isEmpty(scene) || !Lists.newArrayList(Const.ONLINE_MANAGER,Const.ONLINE_RISK).contains(scene)){
            throw new Exception("scene参数不可为空");
        }
        if(StringUtils.isEmpty(data_node)){
            throw new Exception("uid参数不可为空");
        }
    }
}
