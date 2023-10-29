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

    public static void flush(BufferedWriter bw) throws IOException {
        bw.flush();
        bw.close();
    }


    public static List<String> readString(File file, Charset charset) throws IOException {
        return Files.readLines(file, charset);
    }

    public static List<String> readStringSplit(File file, Charset charset) throws IOException {
        List<String> result = new ArrayList<>();
        List<String> tmp = Files.readLines(file, charset);
        for (String line: tmp){
            result.add(line.split(",")[0]);
        }
        return result;
    }
}
