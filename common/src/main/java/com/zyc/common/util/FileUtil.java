package com.zyc.common.util;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

    public static void clear(File file) throws IOException {
        Files.newWriter(file, Charset.forName("utf-8")).flush();
    }

    public static void writeString(File file, String line) throws IOException {
        Files.write(line.getBytes("utf-8"), file);
    }

    public static void appendString(File file, String line) throws IOException {
        cn.hutool.core.io.FileUtil.appendString(line+cn.hutool.core.io.FileUtil.getLineSeparator(), file, "utf-8");
    }

    public static void touch(File file) throws IOException {
        if(!cn.hutool.core.io.FileUtil.exist(file)){
            cn.hutool.core.io.FileUtil.touch(file);
        }
    }

    public static List<String> readString(File file, Charset charset) throws IOException {
        return Files.readLines(file, charset);
    }

    /**
     * 读取文件解析
     * 当前函数仅适用于用于读取策略结果
     * 文件内容格式 按逗号分割,依次如下, uid,status,start_time
     * @param file
     * @param charset
     * @param status
     * @return
     * @throws IOException
     */
    public static List<String> readStringSplit(File file, Charset charset, String status) throws IOException {
        List<String> result = new ArrayList<>();
        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            String[] row = line.split(",");
            if(row.length>2){
                if(status.equalsIgnoreCase(Const.FILE_STATUS_ALL)){
                    result.add(row[0]);
                }else{
                    if(row[1].equalsIgnoreCase(status)){
                        result.add(row[0]);
                    }
                }
            }else{
                result.add(row[0]);
            }
        }
        return result;
    }
}
