package com.zyc.common.util;

import com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

    public static BufferedWriter createBufferedWriter(File file, Charset charset) throws FileNotFoundException {
        return Files.newWriter(file, charset);
    }

    public static void writeString(BufferedWriter bw,String line) throws IOException {
       bw.write(line);
       bw.newLine();
    }

    public static void appendString(BufferedWriter bw,String line) throws IOException {
        bw.append(line);
        bw.newLine();
    }

    public static void flush(BufferedWriter bw) throws IOException {
        bw.flush();
        bw.close();
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
