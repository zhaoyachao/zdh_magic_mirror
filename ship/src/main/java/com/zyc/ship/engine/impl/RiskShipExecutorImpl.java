package com.zyc.ship.engine.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ArrayUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zyc.common.entity.FunctionInfo;
import com.zyc.common.entity.InstanceType;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.groovy.GroovyFactory;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.ship.disruptor.ShipEvent;
import com.zyc.ship.disruptor.ShipExecutor;
import com.zyc.ship.disruptor.ShipResult;
import com.zyc.ship.entity.*;
import com.zyc.ship.service.impl.CacheFunctionServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RiskShipExecutorImpl implements ShipExecutor {

    private Logger logger= LoggerFactory.getLogger(this.getClass());

    private ShipEvent shipEvent;

    private ShipResult shipResult;

    public RiskShipExecutorImpl(ShipEvent shipEvent, ShipResult shipResult){
        this.shipEvent = shipEvent;
        this.shipResult = shipResult;
    }

    @Override
    public ShipResult execute(StrategyInstance strategyInstance) {
        ShipResult shipResult1 = new RiskShipResultImpl();
        try{
            String tmp = "create";
            logger.info("executor: "+strategyInstance.getStrategy_context());
            shipResult1.setStrategyName(strategyInstance.getStrategy_context());
            String uid = ((ShipCommonInputParam)shipEvent.getInputParam()).getUid();
            String product_code = ((ShipCommonInputParam)shipEvent.getInputParam()).getProduct_code();
            String data_node = ((ShipCommonInputParam)shipEvent.getInputParam()).getData_node();
            Map<String,Object> labelVaues = shipEvent.getLabelValues();
            String instance_type = strategyInstance.getInstance_type();
            JSONObject run_jsmind_data =  JSON.parseObject(strategyInstance.getRun_jsmind_data());
            if(instance_type.equalsIgnoreCase(InstanceType.LABEL.getCode())){
                List<LabelValueConfig> labelValueConfigs = labelParam2LableValueConfig(run_jsmind_data);
                tmp = "success";
                for (LabelValueConfig labelValueConfig:labelValueConfigs){
                    if(!diffLable(labelValueConfig, labelVaues, uid)) {
                        tmp = "error";
                    }
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_RULE.getCode())){
                //不支持人群规则
                tmp = "error";
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_OPERATE.getCode())){
                //到执行器时的运算符,都是可执行的,master disruptor会提前判断
                tmp = "success";
            }else if(instance_type.equalsIgnoreCase(InstanceType.CROWD_FILE.getCode())){
                //不支持大批量人群文件
                tmp = "error";
                String crowd_file_id = run_jsmind_data.getOrDefault("crowd_file","").toString();
                //key: {product_code}_{crowd_file_id}_{uid}
                String key = StringUtils.join(product_code,"_", crowd_file_id, "_", uid);
                Object value = JedisPoolUtil.redisClient().get(key);
                if(value != null){
                    tmp = "success";
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.CUSTOM_LIST.getCode())){
                String name_list_str = run_jsmind_data.getOrDefault("name_list","").toString();
                tmp = String.valueOf(Sets.newHashSet(name_list_str.split(",")).contains(uid));
            }else if(instance_type.equalsIgnoreCase(InstanceType.FILTER.getCode())){
                String[] filters = run_jsmind_data.getString("filter").split(",");
                tmp = "success";
                if(!isHitFilter(filters, shipEvent.getFilterValues(), uid)){
                    tmp = "error";
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.SHUNT.getCode())){
                tmp = "success";
                //校验是否命中分流
                if(!shunt(null, strategyInstance, uid)){
                    tmp = "error";
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.TOUCH.getCode())){
                //触达
            }else if(instance_type.equalsIgnoreCase(InstanceType.ID_MAPPING.getCode())){
                String mapping_code = run_jsmind_data.getString("id_mapping_code");
                String tag_key = "tag_"+mapping_code;
                Object value = labelVaues.get(tag_key);
                if(value == null){
                    tmp = "error";
                }
                shipEvent.getRunParam().put(mapping_code, value);
                tmp = "success";
            }else if(instance_type.equalsIgnoreCase(InstanceType.PLUGIN.getCode())){
                //不支持
                tmp = "error";
            }else if(instance_type.equalsIgnoreCase(InstanceType.MANUAL_CONFIRM.getCode())){
                //不支持
                tmp = "error";
            }else if(instance_type.equalsIgnoreCase(InstanceType.RIGHTS.getCode())){
                //不支持
                tmp = "error";
            }else if(instance_type.equalsIgnoreCase(InstanceType.CODE_BLOCK.getCode())){
                String code_type=run_jsmind_data.getOrDefault("code_type", "").toString();
                String command=run_jsmind_data.getOrDefault("command", "").toString();
                //String hashKey = MD5.create().digestHex(command);
                tmp = "error";
                if(code_type.equalsIgnoreCase("groovy")){
                    Map<String,Object> params = new HashMap<>();
                    params.put("strategy_instance_id", strategyInstance.getId());
                    params.put("strategy_instance", strategyInstance);
                    boolean result =(boolean) GroovyFactory.execExpress(command, params);
                    tmp = String.valueOf(result);
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.DATA_NODE.getCode())){
                //节点
                tmp = "error";
                String s_data_node = run_jsmind_data.getOrDefault("data_node","").toString();
                if(data_node.equalsIgnoreCase(s_data_node)){
                    tmp = "success";
                }
            }else if(instance_type.equalsIgnoreCase(InstanceType.RISK.getCode())){
                //决策信息
                String event_code = run_jsmind_data.getString("rule_id");
                String event_code_result = run_jsmind_data.getString("rule_param");
                tmp = "success";
                shipResult1.setRiskStrategyEventResult(new RiskStrategyEventResult(event_code, event_code_result));
            }else if(instance_type.equalsIgnoreCase(InstanceType.TN.getCode())){
                //根据TN生成,任务(暂不支持,在线策略是否新增一个延迟插件)
                tmp = "error";
            }else if(instance_type.equalsIgnoreCase(InstanceType.FUNCTION.getCode())){
                String function_name = run_jsmind_data.getString("rule_id");
                Gson gson=new Gson();
                List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());

                CacheFunctionServiceImpl cacheFunctionService = new CacheFunctionServiceImpl();
                FunctionInfo functionInfo = cacheFunctionService.selectByFunctionCode(function_name);

                List<String> param_value = new ArrayList<>();
                for(Map map: rule_params){
                    String value = map.get("param_value").toString();
                    param_value.add(value);
                }
                Object res = functionExcute(functionInfo, param_value.toArray(new String[param_value.size()]));
                if(res == null){
                    tmp = "error";
                }else{
                    tmp = "success";
                }
            }else{
                logger.error("暂不支持的经营类型: {}", instance_type);
                shipResult1.setStatus("error");
                tmp = "error";
            }

            shipResult1.setStatus(tmp);
            return shipResult1;
        }catch (Exception e){
            e.printStackTrace();
        }

        return shipResult1;
    }

    public List<LabelValueConfig> labelParam2LableValueConfig(JSONObject jsonObject){
        List<LabelValueConfig> labelValueConfigs = Lists.newArrayList();
        try{
            String rule_param = jsonObject.getString("rule_param");
            String rule_id = jsonObject.getString("rule_id");
            if(!StringUtils.isEmpty(rule_param)){
                List<StrategyLabelParamConfig> strategyLabelParamConfigs = JSON.parseArray(rule_param, StrategyLabelParamConfig.class);
                for (StrategyLabelParamConfig strategyLabelParamConfig:strategyLabelParamConfigs){
                    LabelValueConfig labelValueConfig=new LabelValueConfig();
                    labelValueConfig.setCode(strategyLabelParamConfig.getParam_code());
                    labelValueConfig.setValue(strategyLabelParamConfig.getParam_value());
                    labelValueConfig.setOperate(strategyLabelParamConfig.getParam_operate());
                    labelValueConfig.setValue_format("");
                    labelValueConfig.setValue_type(strategyLabelParamConfig.getParam_type());
                    labelValueConfig.setLabel_code(rule_id);
                    labelValueConfigs.add(labelValueConfig);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return labelValueConfigs;
    }

    /**
     * 标签计算
     * @param labelValueConfig
     * @param labelValues
     * @param uid
     * @return
     */
    public boolean diffLable(LabelValueConfig labelValueConfig, Map<String,Object> labelValues, String uid){
        if(diffValue(labelValues.get(labelValueConfig.getLabel_code()),labelValueConfig)){
            return true;
        }
        return false;
    }


    /**
     *
     * @param r 结果
     * @param labelValueConfig 用户配置参数
     * @return
     */
    public boolean diffValue(Object r,LabelValueConfig labelValueConfig){
        try{
            String code = labelValueConfig.getCode();//此处使用code获取对应的结果,原因可兼容一个标签有多个参数的场景
            String operate = labelValueConfig.getOperate();
            String value_type = labelValueConfig.getValue_type();
            Object value = labelValueConfig.getValue();
            if(r instanceof Map ){
                return diffValue(((Map) r).getOrDefault(code, null), value, value_type, operate);
            }else if(r instanceof String){
                return diffValue(JSONObject.parseObject(r.toString()).getOrDefault(code, null), value, value_type, operate);
            }
            return diffValue(r,value,value_type, operate);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param value_type
     * @param operate
     * @return
     */
    public boolean diffValue(Object lValue, Object uValue, String value_type, String operate){
        try{
            if(value_type.equalsIgnoreCase("int")){
                return diffIntValue(Integer.parseInt(lValue.toString()), uValue.toString(), operate);
            }else if(value_type.equalsIgnoreCase("double")){
                return diffDoubleValue(Double.parseDouble(lValue.toString()), uValue.toString(), operate);
            }else if(value_type.equalsIgnoreCase("long")){
                return diffLongValue(Long.parseLong(lValue.toString()),uValue.toString(),operate);
            }else if(value_type.equalsIgnoreCase("date") || value_type.equalsIgnoreCase("timestamp")){
                return diffDateValue(lValue.toString(),uValue.toString(),operate);
            }else if(value_type.equalsIgnoreCase("string")){
                return diffStringValue(lValue.toString(),uValue.toString(),operate);
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffIntValue(Integer lValue, String uValue, String operate){
        try{

            if(operate.equalsIgnoreCase(">")){
                if(lValue>Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Integer.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffDoubleValue(Double lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Double.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param operate
     * @return
     */
    public boolean diffLongValue(Long lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue == Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue != Long.valueOf(uValue)) return true;
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * 日期类型 规定都是yyyy-MM-dd HH:mm:ss 格式
     * @param lValue 标签结果
     * @param uValue 用户输入参数
     * @param operate
     * @return
     */
    public boolean diffDateValue(String lValue, String uValue, String operate){
        try{
            if(operate.equalsIgnoreCase("relative_time")){
                //相对时间
                //param_value 结构[day|hour|second];3;4 ,表示相对未来3到4天
                //param_value 结构[day|hour|second];-3;-1 ,表示相对过去3天到1天
                String[] uValueArray = uValue.split(";", 3);
                if(uValueArray.length != 3){
                    throw new Exception("相对时间参数格式不正确,格式[day|hour|second];[-]3;[-]4, 负号代表过去");
                }
                String unit = uValueArray[0];
                String start = uValueArray[1];
                String end = uValueArray[2];
                Date cur = new Date();
                long startTime = 0;
                long endTime = 0;
                if(unit.equalsIgnoreCase("day")){
                    startTime = DateUtil.offsetDay(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetDay(cur, Integer.valueOf(end)).getTime();
                }else if(unit.equalsIgnoreCase("hour")){
                    startTime = DateUtil.offsetHour(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetHour(cur, Integer.valueOf(end)).getTime();
                }else if(unit.equalsIgnoreCase("second")){
                    startTime = DateUtil.offsetSecond(cur, Integer.valueOf(start)).getTime();
                    endTime = DateUtil.offsetSecond(cur, Integer.valueOf(end)).getTime();
                }
                long o = DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
                if(o >=startTime && o < endTime){
                    return true;
                }
                return false;
            }
            if(operate.equalsIgnoreCase("in")){
                long o =  DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
                long startTime = 0;
                long endTime = 0;
                String[] uValueArray = uValue.split(";", 2);
                String start = uValueArray[0];
                String end = uValueArray[1];
                startTime = DateUtil.parse(start, DatePattern.NORM_DATETIME_PATTERN).getTime();
                endTime = DateUtil.parse(end, DatePattern.NORM_DATETIME_PATTERN).getTime();
                if(o>=startTime && o < endTime){
                    return true;
                }
                return false;
            }
            long o = DateUtil.parse(lValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
            long t = DateUtil.parse(uValue, DatePattern.NORM_DATETIME_PATTERN).getTime();
            if(operate.equalsIgnoreCase(">")){
                if(o>t) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(o<t) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(o>=t) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(o<=t) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(o==t) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(o!=t) return true;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffStringValue(String lValue, String uValue, String operate){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase(">")){
                if(r<0) return true;
            }else if(operate.equalsIgnoreCase("<")){
                if(r>0) return true;
            }else if(operate.equalsIgnoreCase(">=")){
                if(r<=0) return true;
            }else if(operate.equalsIgnoreCase("<=")){
                if(r>=0) return true;
            }else if(operate.equalsIgnoreCase("=")){
                if(r==0) return true;
            }else if(operate.equalsIgnoreCase("!=")){
                if(r!=0) return true;
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean isHitFilter(String[] filters, Map<String,Object> filterValues, String uid){
        if(filters==null || filters.length == 0){
            return true;
        }
        for (String filter: filters){
            if(filterValues.containsKey(filter)){
                return false;
            }
        }
        return true;
    }

    public boolean shunt(StrategyGroupInstance strategyGroup, StrategyInstance strategyInstance, String uid){
        try{
            JSONObject jsonObject = JSON.parseObject(strategyInstance.getRun_jsmind_data());
            String shunt_param_str=jsonObject.get("shunt_param").toString();
            List<Map> shunt_params = JSON.parseArray(shunt_param_str, Map.class);
            Map shunt_param = shunt_params.get(0);
            if(shunt_param == null){
                throw new Exception("分流信息为空");
            }
            String shunt_type = shunt_param.getOrDefault("shunt_type","num").toString();
            if(shunt_type.equalsIgnoreCase("hash")){
                //按hash一致性分流
                String[] shunt_values = shunt_param.getOrDefault("shunt_value","1;100").toString().split(";");
                int start = Integer.parseInt(shunt_values[0]);
                int end = Integer.parseInt(shunt_values[1]);
                int mod= Hashing.consistentHash(uid.hashCode(),100);
                if(mod>=start && mod <= end){
                    return true;
                }
                throw new Exception("hash分流期望值: "+shunt_param.getOrDefault("shunt_value","1;100").toString()+", 实际值: "+ mod);
            }else{
                //非hash分流统一返回false
                throw new Exception("不支持非hash方式之外的分流类型");
            }
        }catch (Exception e){
            logger.error("执行策略id: {}, 策略名称: {}, 策略类型: {}, 分流异常: ", strategyInstance.getId(), strategyInstance.getId(), strategyInstance.getInstance_type(), e.getCause());
            return false;
        }

    }

    public Object functionExcute(FunctionInfo functionInfo, String[] param_value){
        try{
            String function_name = functionInfo.getFunction_name();
            String function_class = functionInfo.getFunction_class();
            String function_load_path = functionInfo.getFunction_load_path();
            String function_script = functionInfo.getFunction_script();
            JSONArray jsonArray = functionInfo.getParam_json_object();

            Map<String, Object> objectMap = new LinkedHashMap<>();
            List<String> params = new ArrayList<>();
            for(int i=0;i<jsonArray.size();i++){
                String param_code = jsonArray.getJSONObject(i).getString("param_code");
                objectMap.put(param_code, param_value[i]);
                params.add(param_code);
            }

            if(CacheFunctionServiceImpl.cacheFunctionInstance.containsKey(function_name)){
                Object clsInstance = CacheFunctionServiceImpl.cacheFunctionInstance.get(function_name);
                if(!StringUtils.isEmpty(function_class)){
                    String[] function_packages = function_class.split(",");
                    String clsName = ArrayUtil.get(function_packages, function_packages.length-1);
                    String clsInstanceName = StringUtils.uncapitalize(clsName);
                    //加载三方工具类
                    if(!StringUtils.isEmpty(function_load_path)){
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap);
                        return ret;
                    }else{
                        objectMap.put(clsInstanceName, clsInstance);
                        function_script = clsInstanceName+"."+function_name+"("+StringUtils.join(params, ",")+")";
                        Object ret = GroovyFactory.execExpress(function_script, objectMap);
                        return ret;
                    }
                }
            }
            if(!StringUtils.isEmpty(function_script)){
                Object ret = GroovyFactory.execExpress(function_script, function_name, objectMap);
                return ret;
            }
        }catch (Exception e){

        }
        return null;
    }
}
