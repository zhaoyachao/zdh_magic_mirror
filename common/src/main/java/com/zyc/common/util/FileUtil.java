package com.zyc.common.util;

import cn.idev.excel.FastExcel;
import cn.idev.excel.read.metadata.ReadSheet;
import cn.idev.excel.support.ExcelTypeEnum;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.zyc.common.entity.DataPipe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FileUtil {

    public static void clear(File file) throws IOException {
        Files.newWriter(file, Charset.forName("utf-8")).flush();
    }

    public static void writeString(File file, String line) throws IOException {
        Files.write(line.getBytes("utf-8"), file);
    }

    public static void appendString(File file, String line) throws IOException {
        Files.asCharSink(file, Charset.forName("utf-8"),FileWriteMode.APPEND).write(line);
        Files.asCharSink(file, Charset.forName("utf-8"),FileWriteMode.APPEND).write(System.lineSeparator());
    }

    public static void touch(File file) throws IOException {
        Files.touch(file);
    }

    public static List<String> readString(File file, Charset charset) throws IOException {
        return Files.readLines(file, charset);
    }

    /**
     * 读取文件解析
     * 当前函数仅适用于用于读取策略结果
     * 文件内容格式 按制表符分割,依次如下, uid,status,start_time
     * @param file
     * @param charset
     * @param status
     * @return
     * @throws IOException
     */
    public static List<DataPipe> readStringSplit(File file, Charset charset, String status, String split) throws IOException {
        return DataPipe.readStringSplit(file, charset, status, split);
    }

    public static List<String> readTextSplit(File file, Charset charset, String split) throws IOException {
        List<String> result = new ArrayList<>();
        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            String[] lines = line.split(split);
            result.add(lines[0]);
        }
        return result;
    }

    public static List<String> readExcelSplit(File file, String excelType, Charset charset, String status) throws IOException {
        List<String> result = new ArrayList<>();
        ExcelTypeEnum excelTypeEnum = ExcelTypeEnum.XLS;
        if(excelType.endsWith("xlsx")){
            excelTypeEnum = ExcelTypeEnum.XLSX;
        }
        List<Map<Integer, Object>> tmp = FastExcel.read(file).headRowNumber(0).excelType(excelTypeEnum).doReadAllSync();
        for (Map<Integer, Object> line: tmp){
            if(line.size()>2){
                if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                    result.add(line.get(0).toString());
                }else{
                    if(line.get(1).toString().equalsIgnoreCase(status)){
                        result.add(line.get(0).toString());
                    }
                }
            }else{
                result.add(line.get(0).toString());
            }
        }
        return result;
    }
}
