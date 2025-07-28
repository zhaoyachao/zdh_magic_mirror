package com.zyc.magic_mirror.common.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5 工具类，支持字符串和文件的MD5计算
 */
public class MD5Util {
    // MD5算法名称
    private static final String ALGORITHM = "MD5";
    // 十六进制字符数组
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /**
     * 计算字符串的MD5值（小写32位）
     * @param str 输入字符串
     * @return MD5十六进制字符串
     */
    public static String getMD5(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("输入字符串不能为空");
        }
        return getMD5(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 计算字节数组的MD5值
     * @param bytes 输入字节数组
     * @return MD5十六进制字符串
     */
    public static String getMD5(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance(ALGORITHM);
            byte[] digest = md.digest(bytes);
            return bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法不支持", e);
        }
    }

    /**
     * 计算文件的MD5值
     * @param file 输入文件
     * @return MD5十六进制字符串
     * @throws IOException 文件操作异常
     */
    public static String getFileMD5(File file) throws IOException {
        if (file == null || !file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("无效的文件");
        }

        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            MessageDigest md = MessageDigest.getInstance(ALGORITHM);
            byte[] buffer = new byte[8192];
            int len;
            while ((len = is.read(buffer)) != -1) {
                md.update(buffer, 0, len);
            }
            return bytesToHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5算法不支持", e);
        }
    }

    /**
     * 将字节数组转换为十六进制字符串
     * @param bytes 字节数组
     * @return 十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            chars[i * 2] = HEX_CHARS[b >>> 4];  // 高4位
            chars[i * 2 + 1] = HEX_CHARS[b & 0x0F];  // 低4位
        }
        return new String(chars);
    }

    // 测试方法
    public static void main(String[] args) throws IOException {
        // 测试字符串MD5
        String testStr = "Hello MD5";
        String strMd5 = getMD5(testStr);
        System.out.println("字符串 \"" + testStr + "\" 的MD5: " + strMd5);
    }
}
    