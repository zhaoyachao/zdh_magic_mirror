package com.zyc.label.calculate.impl;


import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.zyc.common.util.FileUtil;
import com.zyc.common.util.MinioUtil;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class LabelCalculateImplTest {

    @Test
    public void testList(){

        long start=System.currentTimeMillis();
        List<String> a=new ArrayList<>();
        List<String> b=new ArrayList<>();

        for(int i=0;i<5000000;i++){
            a.add("aaaaaaaaaaaaa"+i);
            b.add("bbbbbbbbbbbbb"+i);
        }

        System.out.println(a.size());
        System.out.println(b.size());
        System.out.println("初始化数据: "+(System.currentTimeMillis()-start)/1000);

        System.out.println("开始计算交集");
        long start1=System.currentTimeMillis();
        a.retainAll(b);
        System.out.println("结束计算交集"+(System.currentTimeMillis()-start1)/1000);

        create(a, b);

        System.out.println("开始计算并集");
        a.removeAll(b);
        a.addAll(b);
        System.out.println("结束计算并集"+(System.currentTimeMillis()-start1)/1000);
    }

    public void create(List<String> a, List<String> b){
        a=new ArrayList<>();
        b=new ArrayList<>();
        for(int i=0;i<5000000;i++){
            a.add("aaaaaaaaaaaaa"+i);
            b.add("bbbbbbbbbbbbb"+i);
        }

    }

    @Test
    public void testGuava(){

        long start=System.currentTimeMillis();
        List<String> a=new ArrayList<>();
        List<String> b=new ArrayList<>();

        for(int i=0;i<5000000;i++){
            a.add("aaaaaaaaaaaaa"+i);
            b.add("bbbbbbbbbbbbb"+i);
        }

        System.out.println(a.size());
        System.out.println(b.size());
        System.out.println("初始化数据: "+(System.currentTimeMillis()-start)/1000);

        System.out.println("开始计算交集");
        long start1=System.currentTimeMillis();

        Set<String> sets = Sets.newHashSet(a);
        Set<String> sets2 = Sets.newHashSet(b);
        // 交集
        Sets.SetView<String> intersection = Sets.intersection(sets, sets2);
        System.out.println(intersection.size());
        System.out.println("结束计算交集"+(System.currentTimeMillis()-start1)/1000);

        long start2=System.currentTimeMillis();
        System.out.println("开始计算并集");
        Sets.union(sets,sets2);

        System.out.println("结束计算并集"+(System.currentTimeMillis()-start2)/1000);


        long start3=System.currentTimeMillis();
        System.out.println("开始计算差集");
        Set<String> rest = Sets.difference(sets,sets2);

        System.out.println("结束计算差集"+(System.currentTimeMillis()-start3)/1000);

    }


    @Test
    public void testGuavaIdMapping(){

        long start=System.currentTimeMillis();
        Set<String> a = Sets.newHashSet();
        Map<String,String> b = Maps.newHashMap();
        for(int i=0;i<10000000;i++){
            a.add("aaaaaaaaaaaaa"+i);
            b.put("aaaaaaaaaaaaa"+i,"dddddddddddddd"+i);
        }

        System.out.println(a.size());
        System.out.println(b.size());
        System.out.println("初始化数据: "+(System.currentTimeMillis()-start)/1000);

        System.out.println("开始计算映射");
        Set<String> sets = Sets.newHashSet();
        long start1=System.currentTimeMillis();
        Iterator<String> a1 = a.iterator();
        while (a1.hasNext()){
            String key = a1.next();
            if(b.containsKey(key)){
                sets.add(b.get(key));
            }
        }
        System.out.println(sets.size());
        System.out.println("结束计算差集"+(System.currentTimeMillis()-start1)/1000);

    }

    @Test
    public void testFileWrite() throws IOException {
        File f = new File("/home/data/w1");
//        BufferedWriter bw = FileUtil.createBufferedWriter(f, Charset.forName("utf-8"));
//        FileUtil.writeString(bw, "测试1");
//        FileUtil.writeString(bw, "测试1");
//        FileUtil.flush(bw);
    }

    @Test
    public void testMinio() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, InsufficientDataException, ErrorResponseException {

        MinioClient minioClient = MinioUtil.buildMinioClient("minio_zyc", "minio_zyc", "http://111.173.105.158:9000");


        String bucket = "zdh-magic-mirror";
        MinioUtil.createBucket(minioClient, bucket, "cn-north-1");

        String file_name = "/home/zyc/label/1243700840024248320/1243700886891401216/1243700887126282249";
        //MinioUtil.putObject(minioClient, bucket, "cn-north-1", "application/otcet-stream",file_name,file_name ,null);
        MinioUtil.removeObject(minioClient, bucket, "cn-north-1", file_name);

    }
}
