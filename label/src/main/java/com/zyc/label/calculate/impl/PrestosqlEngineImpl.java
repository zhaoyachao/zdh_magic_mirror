package com.zyc.label.calculate.impl;

import com.zyc.label.calculate.SqlEngine;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * presto 引擎表达式实现
 */
public class PrestosqlEngineImpl implements SqlEngine {

    @Override
    public String buildExpr(String param_value, String param_type, String param_code, String param_operate) throws Exception {
        if(param_operate.equalsIgnoreCase("relative_time")){
            return buildExprByRelativeTime(param_value, param_type, param_code);
        }
        throw new Exception("参数:"+param_code+"不支持的操作符");
    }

    private String buildExprByRelativeTime(String param_value, String param_type, String param_code) throws Exception {
        if(!StringUtils.isEmpty(param_value)){
            String[] param_values = param_value.split(";");
            String unit="";
            String start = "";
            String end = "";
            if(param_values.length !=3){
                throw new Exception("参数:"+param_code+"相对时间配置错误,配置信息为空");
            }

            unit = param_values[0];
            if(!Arrays.asList(new String[]{"day","hour","second"}).contains(unit)){
                //抛异常
                throw new Exception("参数:"+param_code+"相对时间配置错误,时间单位,必须放到首位");
            }
            start = param_values[1];
            end = param_values[2];

            if(Integer.parseInt(start)>Integer.parseInt(end)){
                //抛异常,开始时间 只能小于结束时间
                throw new Exception("参数:"+param_code+"相对时间配置错误,开始时间不可大于结束时间");
            }
            String value="";
            if(unit.equalsIgnoreCase("day")){
                if(param_type!=null && (param_type.equalsIgnoreCase("date") || param_type.equalsIgnoreCase("timestamp"))){
                    value=String.format("date_diff('day',%s, current_date)>="+start+" and date_diff('day',%s,current_date)<="+end, param_code,param_code);
                    if(start.contains("-") || end.contains("-")){
                        value=String.format("date_diff('day',current_date,%s)>="+start+" and date_diff('day',current_date,%s)<="+end, param_code,param_code);
                    }
                }else if(param_type!=null && param_type.equalsIgnoreCase("ts")){
                    //数据是时间戳类型,需要通过时间戳转日期函数
                    value="date_diff('day',format_datetime(from_unixtime(try_cast("+param_code+" as bigint)),'yyyy-MM-dd'),current_date)>="+start+" and date_diff('day',format_datetime(from_unixtime(try_cast("+param_code+" as bigint)),'yyyy-MM-dd'),current_date)<= "+end;
                    if(start.contains("-") || end.contains("-")){
                        value="date_diff('day',current_date, format_datetime(from_unixtime(try_cast("+param_code+" as bigint)),'yyyy-MM-dd'))>="+start+" and date_diff('day',current_date,format_datetime(from_unixtime(try_cast("+param_code+" as bigint)),'yyyy-MM-dd'))<= "+end;
                    }
                }

            }else if(unit.equalsIgnoreCase("hour")){
                if(param_type!=null && (param_type.equalsIgnoreCase("date") || param_type.equalsIgnoreCase("timestamp"))){
                    value=String.format("date_diff('hour',%s, current_timestamp)>="+start+" and date_diff('hour',%s,current_timestamp)<="+end, param_code,param_code);
                    //value=String.format("TIMESTAMPDIFF(HOUR, current_timestamp(), %s)>="+start+" and TIMESTAMPDIFF(HOUR, current_timestamp(), %s)<="+end, param_code,param_code);
                    if(start.contains("-") || end.contains("-")){
                        value=String.format("date_diff('hour', current_timestamp,%s)>="+start+" and date_diff('hour',current_timestamp,%s)<="+end, param_code,param_code);
                        //value=String.format("TIMESTAMPDIFF(HOUR, %s, current_timestamp())>="+start+" and TIMESTAMPDIFF(HOUR, %s, current_timestamp())<="+end, param_code,param_code);
                    }
                }else if(param_type!=null && !param_type.equalsIgnoreCase("ts")){
                    value="date_diff('hour',from_unixtime(try_cast("+param_code+" as bigint)),current_timestamp)>="+start+" and date_diff('day',from_unixtime(try_cast("+param_code+" as bigint)),current_timestamp)<= "+end;
                    //value="TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UNIXTIME("+param_code+", 'yyyy-MM-dd HH:mm:ss'))>="+start+" and TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UNIXTIME("+param_code+", 'yyyy-MM-dd HH:mm:ss'))<="+end;

                    if(start.contains("-") || end.contains("-")){
                        value="date_diff('hour',current_timestamp,from_unixtime(try_cast("+param_code+" as bigint)))>="+start+" and date_diff('day',current_timestamp,from_unixtime(try_cast("+param_code+" as bigint)))<= "+end;
                        //value="TIMESTAMPDIFF(HOUR, FROM_UNIXTIME("+param_code+", 'yyyy-MM-dd HH:mm:ss'), current_timestamp())>="+start+" and TIMESTAMPDIFF(HOUR, FROM_UNIXTIME("+param_code+", 'yyyy-MM-dd HH:mm:ss'), current_timestamp())<="+end;
                    }
                }

            }else if(unit.equalsIgnoreCase("second")){
                //此处暂不实现
                throw new Exception("参数:"+param_code+"相对时间配置错误,暂不支持秒级单位处理");
            }

            return value;
        }
        throw new Exception("参数:"+param_code+"相对时间配置为空");
    }
}
