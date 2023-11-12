package com.zyc.label.calculate.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hubspot.jinjava.Jinjava;
import com.zyc.common.entity.DataSourcesInfo;
import com.zyc.common.entity.LabelInfo;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.entity.StrategyLogInfo;
import com.zyc.common.redis.JedisPoolUtil;
import com.zyc.common.util.DBUtil;
import com.zyc.common.util.LogUtil;
import com.zyc.label.calculate.LabelCalculate;
import com.zyc.label.service.impl.DataSourcesServiceImpl;
import com.zyc.label.service.impl.LabelServiceImpl;
import com.zyc.label.service.impl.StrategyInstanceServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    }

    @Override
    public void run() {
        atomicInteger.incrementAndGet();
        StrategyInstanceServiceImpl strategyInstanceService=new StrategyInstanceServiceImpl();
        LabelServiceImpl labelService=new LabelServiceImpl();
        DataSourcesServiceImpl dataSourcesService=new DataSourcesServiceImpl();
        //唯一任务ID
        String id=this.param.get("id").toString();
        String group_id=this.param.get("group_id").toString();
        String strategy_id=this.param.get("strategy_id").toString();
        String group_instance_id=this.param.get("group_instance_id").toString();
        StrategyLogInfo strategyLogInfo = new StrategyLogInfo();
        strategyLogInfo.setStrategy_group_id(group_id);
        strategyLogInfo.setStrategy_id(strategy_id);
        strategyLogInfo.setStrategy_instance_id(id);
        strategyLogInfo.setStrategy_group_instance_id(group_instance_id);
        String logStr="";
        String file_path = "";
        try{

            //获取标签code
            Map run_jsmind_data = JSON.parseObject(this.param.get("run_jsmind_data").toString(), Map.class);
            String label_code=run_jsmind_data.get("rule_id").toString();
            String is_disenable=run_jsmind_data.getOrDefault("is_disenable","false").toString();//true:禁用,false:未禁用

            //调度逻辑时间,yyyy-MM-dd HH:mm:ss
            String cur_time=this.param.get("cur_time").toString();

            if(dbConfig==null){
                throw new Exception("标签信息数据库配置异常");
            }
            String driver=dbConfig.get("driver");
            String url=dbConfig.get("url");
            String username=dbConfig.get("user");
            String password=dbConfig.get("password");
            String base_path=dbConfig.get("file.path");


            Set<String> rowsStr = Sets.newHashSet();
            //判断是否跳过类的策略,通过is_disenable=true,禁用的任务直接拉取上游任务的结果,并集(),交集(),排除()
            if(is_disenable.equalsIgnoreCase("true")){
                //当前策略跳过状态,则不计算当前策略信息,且跳过校验
            }else{
                //解析参数,生成人群
                LabelInfo labelInfo = labelService.selectByCode(label_code);
                if(labelInfo==null){
                    throw new Exception("无法找到标签信息");
                }
                if(!labelInfo.getStatus().equalsIgnoreCase("1")){
                    throw new Exception("标签未启用");
                }
                //校验标签底层数据是否准备完成
                if(Boolean.valueOf(dbConfig.getOrDefault("label.check.dep", "false"))){
                    boolean is_dep = checkLabelDep(labelInfo, cur_time);
                    if(!is_dep){
                        //依赖未完成,直接返回,此处应该打印日志
                        logger.info("task: {}, labele: {} ,depend data is not found, please wait retry", id, label_code);
                        return ;
                    }
                }
                //生成参数
                Gson gson=new Gson();
                List<Map> rule_params = gson.fromJson(run_jsmind_data.get("rule_param").toString(), new TypeToken<List<Map>>(){}.getType());
                Map<String, String> jinJavaParam=getJinJavaParam(rule_params,labelInfo);
                jinJavaParam.put("cur_time", cur_time);
                logStr = StrUtil.format("task: {}, param: {}", id, jinJavaParam);
                LogUtil.info(strategy_id, id, logStr);

                String data_sources_choose_input = labelInfo.getData_sources_choose_input();

                DataSourcesInfo dataSourcesInfo = dataSourcesService.selectById(data_sources_choose_input);
                //获取sql模板
                String sql=labelInfo.getLabel_expression();
                Jinjava jinjava=new Jinjava();

                String new_sql = jinjava.render(sql, jinJavaParam);
                logStr = StrUtil.format("task: {}, sql: {}", id, new_sql);
                LogUtil.info(strategy_id, id, logStr);
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

            }


            Set<String> rs=Sets.newHashSet() ;
            //检查标签上游
            String pre_tasks = this.param.get("pre_tasks").toString();
            List<StrategyInstance> strategyInstances = strategyInstanceService.selectByIds(pre_tasks.split(","));
            String file_dir= getFileDir(base_path,group_id,group_instance_id);
            String operate=run_jsmind_data.get("operate").toString();
            List<String> pre_tasks_list = Lists.newArrayList();
            if(!StringUtils.isEmpty(pre_tasks)){
                pre_tasks_list = Lists.newArrayList(pre_tasks.split(","));
            }
            rs = calculate(file_dir, pre_tasks_list, operate, rowsStr, strategyInstances, is_disenable);

            //执行sql,返回数据写文件
            file_path= getFilePath(base_path,group_id,group_instance_id,id);

            String save_path = writeFile(id,file_path, rs);
            logStr = StrUtil.format("task: {}, write finish, file: {}", id, save_path);
            LogUtil.info(strategy_id, id, logStr);
            setStatus(id, "finish");
            strategyLogInfo.setStatus("1");
            strategyLogInfo.setSuccess_num(String.valueOf(rs.size()));
            logStr = StrUtil.format("task: {}, update status finish", id);
            LogUtil.info(strategy_id, id, logStr);
            //根据计算引擎 执行,以spark sql 执行
            //new_sql = "insert overwrite table label_detail PARTITION(task_id='"+id+"') "+new_sql;

            String sparkDriver="org.apache.hive.jdbc.HiveDriver";
            String sparkUrl="jdbc:hive2://192.168.110.10:10000/default";
            String sparkUser="";
            String sparkPassword="";

            System.err.println("计算引擎执行待实现,当前以本地文件做存储");

        }catch (Exception e){
            writeEmptyFile(file_path);
            setStatus(id, "error");
            LogUtil.error(strategy_id, id, e.getMessage());
            //执行失败,更新标签任务失败
            e.printStackTrace();
        }finally {
            atomicInteger.decrementAndGet();
        }
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
                format = "yyyy-MM-dd HH:mm:ss";
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
            e.printStackTrace();
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
        if(engine.equalsIgnoreCase("mysql")){

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

}
