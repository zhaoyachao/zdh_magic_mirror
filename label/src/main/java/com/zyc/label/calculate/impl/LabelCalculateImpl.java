package com.zyc.label.calculate.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.DataSourcesInfo;
import com.zyc.common.entity.LabelInfo;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.Const;
import com.zyc.common.util.DBUtil;
import com.zyc.common.util.HttpClientUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.label.calculate.LabelCalculate;
import com.zyc.label.service.impl.DataSourcesServiceImpl;
import com.zyc.label.service.impl.LabelServiceImpl;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import com.zyc.rqueue.RQueueClient;
import com.zyc.rqueue.RQueueManager;
import com.zyc.rqueue.RQueueMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 标签计算实现
 */
public class LabelCalculateImpl extends BaseCalculate implements LabelCalculate{
    private static Logger logger= LoggerFactory.getLogger(LabelCalculateImpl.class);

    /**
     * {
     * 	"owner": "zyc",
     * 	"schedule_source": "2",
     * 	"strategy_context": "(年龄 in 19)",
     * 	"create_time": 1658629372000,
     * 	"jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *        },
     * 	"run_time": 1660993147000,
     * 	"group_instance_id": "1010624036146778112",
     * 	"cur_time": 1660993145000,
     * 	"pre_tasks": "",
     * 	"group_context": "测试策略组",
     * 	"priority": "",
     * 	"is_disenable": "false",
     * 	"is_delete": "0",
     * 	"run_jsmind_data": {
     * 		"rule_expression_cn": " (年龄 in 19)",
     * 		"rule_param": "[{\"param_code\":\"age\",\"param_context\":\"年龄\",\"param_operate\":\"in\",\"param_value\":\"19\"}]",
     * 		"type": "label",
     * 		"is_disenable": "false",
     * 		"time_out": "86400",
     * 		"rule_context": " (年龄 in 19)",
     * 		"positionX": 44,
     * 		"rule_id": "age",
     * 		"positionY": 11,
     * 		"operate": "and",
     * 		"name": "(年龄 in 19)",
     * 		"more_task": "label",
     * 		"id": "4d7_8e6_9652_37",
     * 		"divId": "4d7_8e6_9652_37"
     *    },
     * 	"start_time": 1660993145000,
     * 	"update_time": 1660993147000,
     * 	"group_id": "测试策略组",
     * 	"misfire": "0",
     * 	"next_tasks": "1010624036201304064",
     * 	"id": "1010624036209692673",
     * 	"instance_type": "label",
     * 	"depend_level": "0",
     * 	"status": "create"
     * }
     */
    private Map<String,Object> param=new HashMap<String, Object>();
    private AtomicInteger atomicInteger;
    private Map<String,String> dbConfig=new HashMap<String, String>();

    public LabelCalculateImpl(Map<String, Object> param, AtomicInteger atomicInteger,Properties dbConfig){
        this.param=param;
        this.atomicInteger=atomicInteger;
        this.dbConfig=new HashMap<>((Map)dbConfig);
        getSftpUtil(this.dbConfig);
    }

    @Override
    public boolean checkSftp() {
        return Boolean.valueOf(this.dbConfig.getOrDefault("sftp.enable", "false"));
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        LabelServiceImpl labelService=new LabelServiceImpl();

        StrategyLogInfo strategyLogInfo = init(this.param, this.dbConfig);
        String logStr="";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String label_code=run_jsmind_data.get("rule_id").toString();
            String label_use_type=run_jsmind_data.getOrDefault("label_use_type", "batch").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            String driver=dbConfig.get("driver");
            String url=dbConfig.get("url");
            String username=dbConfig.get("user");
            String password=dbConfig.get("password");
            String base_path=dbConfig.get("file.path");
            String label_url=dbConfig.get("label.http.url");
            LabelInfo labelInfo = labelService.selectByCode(label_code, label_use_type);
            Set<String> rowsStr = Sets.newHashSet();
            //判断是否跳过类的策略,通过is_disenable=true,禁用的任务直接拉取上游任务的结果,并集(),交集(),排除()
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //解析参数,生成人群
                if(labelInfo==null){
                    throw new Exception("无法找到标签信息");
                }
                if(!labelInfo.getStatus().equalsIgnoreCase("1")){
                    throw new Exception("标签未启用,标签名称: "+labelInfo.getLabel_context());
                }

                if(label_use_type.equalsIgnoreCase("batch")){
                    //校验标签底层数据是否准备完成
                    if(Boolean.valueOf(dbConfig.getOrDefault("label.check.dep", "false"))){
                        boolean is_dep = checkLabelDep(labelInfo, DateUtil.format(strategyLogInfo.getCur_time(), DatePattern.NORM_DATETIME_PATTERN));
                        if(!is_dep){
                            //依赖未完成,直接返回,此处应该打印日志
                            logger.warn("task: {}, labele: {} ,depend data is not found, please wait retry", strategyLogInfo.getStrategy_instance_id(), labelInfo.getLabel_code());
                            //当前任务写入延迟队列,5分钟后重置状态
                            RQueueClient rQueueClient = RQueueManager.getRQueueClient(Const.LABEL_DOUBLE_CHECK_DEPENDS_QUEUE_NAME, RQueueMode.DELAYEDQUEUE);
                            rQueueClient.offer(strategyLogInfo.getStrategy_instance_id(), 5L, TimeUnit.MINUTES);
                            return ;
                        }
                    }

                    rowsStr = offlineLabel(is_disenable, run_jsmind_data, labelInfo, strategyLogInfo);
                }
            }

            Set<String> rs=Sets.newHashSet() ;
            String file_dir= getFileDir(strategyLogInfo.getBase_path(), strategyLogInfo.getStrategy_group_id(),
                    strategyLogInfo.getStrategy_group_instance_id());
            //解析上游任务并和当前节点数据做运算
            if(label_use_type.equalsIgnoreCase("batch")) {
                rs = calculateCommon("offline",rowsStr, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);
            }else if(label_use_type.equalsIgnoreCase("single")){
                //使用实时标签,需要确保当前标签是子层标签
                rs = calculateCommon("online",rowsStr, is_disenable, file_dir, this.param, run_jsmind_data, strategyInstanceService);

                if(rs == null || rs.size()==0){

                }else{
                    //遍历结果,使用在线标签逻辑运算
                    rs = onlineLabel(run_jsmind_data, rs, label_url, labelInfo);
                }
            }

            writeFileAndPrintLogAndUpdateStatus2Finish(strategyLogInfo,rs);
            writeRocksdb(strategyLogInfo.getFile_rocksdb_path(), strategyLogInfo.getStrategy_instance_id(), rs, Const.STATUS_FINISH);

        }catch (Exception e){
            writeEmptyFileAndStatus(strategyLogInfo);
            LogUtil.error(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), e.getMessage());
            //执行失败,更新标签任务失败
            logger.error("label label run error: ", e);
        }finally {
            atomicInteger.decrementAndGet();
            removeTask(strategyLogInfo.getStrategy_instance_id());
        }
    }


    public Set<String> offlineLabel(String is_disenable, Map run_jsmind_data, LabelInfo labelInfo, StrategyLogInfo strategyLogInfo) throws Exception {
        Set<String> rowsStr = Sets.newHashSet();
        DataSourcesServiceImpl dataSourcesService=new DataSourcesServiceImpl();

        //生成参数
        Gson gson=new Gson();
        List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());
        Map<String, String> jinJavaParam=getJinJavaParam(rule_params,labelInfo);
        jinJavaParam.put("cur_time", DateUtil.format(strategyLogInfo.getCur_time(), DatePattern.NORM_DATETIME_PATTERN));
        String logStr = StrUtil.format("task: {}, param: {}", strategyLogInfo.getStrategy_instance_id(), jinJavaParam);
        LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);

        String data_sources_choose_input = labelInfo.getData_sources_choose_input();

        DataSourcesInfo dataSourcesInfo = dataSourcesService.selectById(data_sources_choose_input);
        //获取sql模板
        String sql=labelInfo.getLabel_expression();
        Jinjava jinjava=new Jinjava();

        String new_sql = jinjava.render(sql, jinJavaParam);
        logStr = StrUtil.format("task: {}, sql: {}", strategyLogInfo.getStrategy_instance_id(), new_sql);
        LogUtil.info(strategyLogInfo.getStrategy_id(), strategyLogInfo.getStrategy_instance_id(), logStr);
        List<Map<String,Object>> rows =new ArrayList<>();
        if(is_disenable.equalsIgnoreCase("true")){
            //禁用任务不做处理,认为结果为空
        }else{
            rows = execute_sql(new_sql, this.param, dataSourcesInfo);
            if(rows==null || rows.size()==0){
                System.err.println("数据执行为空");
            }
        }
        for(Map<String,Object> r: rows){
            rowsStr.add(String.join(",",r.values().toArray(new String[]{})));
        }

        return rowsStr;
    }


    public Set<String> onlineLabel(Map run_jsmind_data,Set<String> rs, String label_url, LabelInfo labelInfo){
        Set<String> tmp = Sets.newHashSet();
        Gson gson=new Gson();
        List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());
        for(String r: rs){
            Map<String, Object> result = getLabel(label_url, r, labelInfo.getProduct_code(), labelInfo.getLabel_code());
            for(Map param_map: rule_params){
                String param_code = param_map.getOrDefault("param_code", "").toString();
                String param_value = param_map.getOrDefault("param_value", "").toString();
                String param_operate = param_map.getOrDefault("param_operate", "").toString();
                String param_type = param_map.getOrDefault("param_type", "").toString();
                String param_return_type = param_map.getOrDefault("param_return_type", "").toString();

                String lvalue = result.getOrDefault(param_code, "").toString();
                if(!diffValue(lvalue, param_value, param_type, param_operate, param_return_type)){
                    break;
                }
            }
            tmp.add(r);
        }
        return tmp;
    }

    public Map<String, Object> getLabel(String label_url, String uid, String product_code, String variable){
        Map<String, Object> result = new HashMap<>();
        try{
            cn.hutool.json.JSONObject jsonObject = JSONUtil.createObj();
            jsonObject.putOpt("uid", uid);
            jsonObject.putOpt("product_code", product_code);
            jsonObject.putOpt("variable", variable);
            result = JSON.parseObject(HttpClientUtil.postJson(label_url, jsonObject.toString()),Map.class);
            return result;
        }catch (Exception e){

        }
        return result;
    }
    /**
     * 检查标签依赖
     * @param labelInfo
     * @param cur_time
     * @return
     * @throws Exception
     */
    public boolean checkLabelDep(LabelInfo labelInfo, String cur_time) throws Exception{
        try{
            String format = "yyyy-MM-dd 00:00:00";
            String label_data_time_effect = labelInfo.getLabel_data_time_effect();//day,hour,second
            if(label_data_time_effect.equalsIgnoreCase("day")){

            }else if(label_data_time_effect.equalsIgnoreCase("hour")){
                format = "yyyy-MM-dd HH:00:00";
            }else if(label_data_time_effect.equalsIgnoreCase("sencod")){
                format = "yyyy-MM-dd HH:mm:00";
            }else{

            }
            Date cur_date = FastDateFormat.getInstance(format).parse(cur_time);
            String cur_date_str  = FastDateFormat.getInstance(format).format(cur_date);
            //根据date 和标签code 查找是否准备完成

            Object ret = JedisPoolUtil.redisClient.hGet(labelInfo.getLabel_code(), cur_date_str);
            if(ret != null){
                return true;
            }
        }catch (Exception e){
            throw e;
        }

        return false;
    }

    public Map<String,String> getJinJavaParam(List<Map> rule_params,LabelInfo labelInfo) throws Exception {
        Map<String, String> jinJavaParam=new HashMap<String, String>();
        if(rule_params != null && rule_params.size()>0){
            for(Map rule_param: rule_params){
                String code = rule_param.get("param_code").toString();
                String operate = rule_param.get("param_operate").toString();
                String param_value = rule_param.get("param_value").toString();
                String param_type = rule_param.get("param_type").toString();

                switch (operate){
                    case "in":
                        List<String> values=new ArrayList<String>();
                        for (String value:param_value.split(";")){
                            if(!StringUtils.isEmpty(value)){
                                if(param_type!=null && !param_type.equalsIgnoreCase("string")){
                                    values.add(value);
                                }else{
                                    values.add("'"+value+"'");
                                }
                            }
                        }
                        if(values.size()>0){
                            jinJavaParam.put(code,code);
                            jinJavaParam.put(code+"_operate","in");
                            jinJavaParam.put(code+"_value","( "+StringUtils.join(values, ",")+" )");
                        }
                        break;
                    case "between":
                        //左闭右开
                        if(!StringUtils.isEmpty(param_value)){
                            String[] p=param_value.split(";",2);
                            if(p.length==2){
                                jinJavaParam.put(code,"");
                                jinJavaParam.put(code+"_operate","");
                                jinJavaParam.put(code+"_value",code+" > '"+p[0]+"' "+ " and <= '"+p[1]+"'");
                            }
                            //此处抛异常
                            throw new Exception("参数:"+code+"时间区间配置错误,时间单位,必须放到首位");
                        }
                        break;
                    case "relative_time":
                        //todo 此处重新实现,不区分未来过去,根据时间加减即可
                        //param_value 结构[day|hour|second];3;4 ,表示相对未来3到4天
                        //param_value 结构[day|hour|second];-3;-1 ,表示相对过去3天到1天
                        if(labelInfo.getLabel_engine().equalsIgnoreCase("mysql")){
                            MysqlEngineImpl mysqlEngine = new MysqlEngineImpl();
                            String expr = mysqlEngine.buildExpr(param_value, param_type, code, operate);
                            jinJavaParam.put(code,expr);
                        }else if(labelInfo.getLabel_engine().equalsIgnoreCase("hive")){
                            HivesqlEngineImpl hivesqlEngine = new HivesqlEngineImpl();
                            String expr = hivesqlEngine.buildExpr(param_value, param_type, code, operate);
                            jinJavaParam.put(code,expr);
                        }else if(labelInfo.getLabel_engine().equalsIgnoreCase("presto")){
                            PrestosqlEngineImpl prestosqlEngine = new PrestosqlEngineImpl();
                            String expr = prestosqlEngine.buildExpr(param_value, param_type, code, operate);
                            jinJavaParam.put(code,expr);
                        }else if(labelInfo.getLabel_engine().equalsIgnoreCase("spark")){
                            SparksqlEngineImpl sparksqlEngine = new SparksqlEngineImpl();
                            String expr = sparksqlEngine.buildExpr(param_value, param_type, code, operate);
                            jinJavaParam.put(code,expr);
                        }else{
                            throw new Exception("暂不支持其他计算引擎");
                        }
                        break;
                    default:
                        if(!StringUtils.isEmpty(param_value)){
                            if(operate.equalsIgnoreCase("=") && param_value.equalsIgnoreCase("null")){
                                jinJavaParam.put(code,code+" is null");
                                jinJavaParam.put(code+"_operate","");
                                jinJavaParam.put(code+"_value","");
                                break;
                            }else  if(operate.equalsIgnoreCase("!=") && param_value.equalsIgnoreCase("null")){
                                jinJavaParam.put(code,code+" is not null");
                                jinJavaParam.put(code+"_operate","");
                                jinJavaParam.put(code+"_value","");
                                break;
                            }
                            jinJavaParam.put(code,code);
                            jinJavaParam.put(code+"_operate",operate);

                            if(param_type!=null && !param_type.equalsIgnoreCase("string")){
                                jinJavaParam.put(code+"_value",param_value);
                            }else{
                                jinJavaParam.put(code+"_value"," '"+param_value+"'");
                            }
                        }
                        break;
                }

            }
        }

        return jinJavaParam;
    }

    public List<Map<String,Object>> execute_sql(String sql, Map<String,Object> label, DataSourcesInfo dataSourcesInfo) throws Exception {
        String engine = label.getOrDefault("engine", "mysql").toString();
        if(engine.equalsIgnoreCase("mysql") || engine.equalsIgnoreCase("hive")
                || engine.equalsIgnoreCase("presto") || engine.equalsIgnoreCase("spark")){

            String sparkDriver=dataSourcesInfo.getDriver();
            String sparkUrl=dataSourcesInfo.getUrl();
            String sparkUser=dataSourcesInfo.getUsername();
            String sparkPassword=dataSourcesInfo.getPassword();
            DBUtil dbUtil=new DBUtil();
            List<Map<String,Object>> list = dbUtil.R5(sparkDriver,sparkUrl, sparkUser, sparkPassword, sql, null);
            return list;
        }
        throw new Exception("不支持的计算引擎:"+engine);
    }


    /**
     *
     * @param lValue 标签返回结果,一般只有一个结果(特殊场景可能会有多个,此处不处理)
     * @param uValue 用户配置的参数,可能是按分号分割的集合
     * @param value_type
     * @param operate
     * @return
     */
    public boolean diffValue(Object lValue, Object uValue, String value_type, String operate, String param_return_type){
        try{
            if(value_type.equalsIgnoreCase("int")){
                return diffIntValue(Integer.parseInt(lValue.toString()), uValue.toString(), operate, param_return_type);
            }else if(value_type.equalsIgnoreCase("double")){
                return diffDoubleValue(Double.parseDouble(lValue.toString()), uValue.toString(), operate, param_return_type);
            }else if(value_type.equalsIgnoreCase("long")){
                return diffLongValue(Long.parseLong(lValue.toString()),uValue.toString(),operate, param_return_type);
            }else if(value_type.equalsIgnoreCase("date") || value_type.equalsIgnoreCase("timestamp")){
                return diffDateValue(lValue.toString(),uValue.toString(),operate, param_return_type);
            }else if(value_type.equalsIgnoreCase("string")){
                return diffStringValue(lValue.toString(),uValue.toString(),operate, param_return_type);
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
    public boolean diffIntValue(Integer lValue, String uValue, String operate, String param_return_type){
        try{

            if(operate.equalsIgnoreCase(">")){
                if(lValue>Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.intValue() == Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.intValue() != Integer.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
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
    public boolean diffDoubleValue(Double lValue, String uValue, String operate, String param_return_type){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.doubleValue() == Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.doubleValue() != Double.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
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
    public boolean diffLongValue(Long lValue, String uValue, String operate, String param_return_type){
        try{
            if(operate.equalsIgnoreCase(">")){
                if(lValue>Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(lValue<Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(lValue>=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(lValue<=Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(lValue.longValue() == Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(lValue.longValue() != Long.valueOf(uValue)) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                Set sets = Sets.newHashSet(uValue.split(";"));
                if(sets.contains(lValue)) {
                    return true;
                }
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
    public boolean diffDateValue(String lValue, String uValue, String operate, String param_return_type){
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
                if(o>t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(o<t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(o>=t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(o<=t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(o==t) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(o!=t) {
                    return true;
                }
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

    public boolean diffStringValue(String lValue, String uValue, String operate, String param_return_type){
        try{
            int r = lValue.compareTo(uValue);
            if(operate.equalsIgnoreCase(">")){
                if(r<0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<")){
                if(r>0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase(">=")){
                if(r<=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("<=")){
                if(r>=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("=")){
                if(r==0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("!=")){
                if(r!=0) {
                    return true;
                }
            }else if(operate.equalsIgnoreCase("in")){
                boolean in = Sets.newHashSet(uValue.split(";|,")).contains(lValue);
                return in;
            }
            return false;
        }catch (Exception e){
            return false;
        }
    }

}
