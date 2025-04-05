package com.zyc.magic_mirror.common.util;

import cn.idev.excel.FastExcel;
import cn.idev.excel.support.ExcelTypeEnum;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.zyc.magic_mirror.common.entity.DataPipe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
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

    public static List<DataPipe> readTextSplit(File file, Charset charset, String split) throws IOException {
        List<DataPipe> result = new ArrayList<>();
        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            DataPipe dataPipe = DataPipe.readStringSplit(line, split);
            result.add(dataPipe);
        }
        return result;
    }

    public static List<DataPipe> readExcelSplit(File file, String excelType, Charset charset, String status) throws IOException {
        List<DataPipe> result = new ArrayList<>();
        ExcelTypeEnum excelTypeEnum = ExcelTypeEnum.XLS;
        if(excelType.endsWith("xlsx")){
            excelTypeEnum = ExcelTypeEnum.XLSX;
        }
        List<Map<Integer, Object>> tmp = FastExcel.read(file).headRowNumber(0).excelType(excelTypeEnum).doReadAllSync();
        for (Map<Integer, Object> line: tmp){
            DataPipe dataPipe = new DataPipe.Builder()
                    .udata(line.containsKey(0)?line.get(0).toString():"")
                    .status(line.containsKey(1) && line.get(1) != null ?line.get(1).toString():"")
                    .status_desc(line.containsKey(2) && line.get(2) != null ?line.get(2).toString():"")
                    .udata_type(line.containsKey(3) && line.get(3) != null ?line.get(3).toString():"")
                    .task_type(line.containsKey(4) && line.get(4) != null ?line.get(4).toString():"")
                    .execute_time(line.containsKey(5) && line.get(5) != null ?line.get(5).toString():"")
                    .ext(line.containsKey(6) && line.get(6) != null ?JsonUtil.toJavaMap(line.get(6).toString()):JsonUtil.createEmptyLinkMap())
                    .build();

            if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                result.add(dataPipe);
            }else{
                if(dataPipe.getStatus().equalsIgnoreCase(status)){
                    result.add(dataPipe);
                }
            }
        }
        return result;
    }

    public static List<String> readTextFirstSplit(File file, Charset charset, String split) throws IOException {
        List<String> result = new ArrayList<>();
        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            String[] lines = line.split(split);
            result.add(lines[0]);
        }
        return result;
    }
}
