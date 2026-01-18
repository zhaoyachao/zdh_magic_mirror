package com.zyc.magic_mirror.common.http;

import javassist.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Java包遍历和类初始化工具类
 */
public class PackageScanner {

    private static Logger logger = LoggerFactory.getLogger(PackageScanner.class);

    /**
     * 自动初始化指定包下所有实现targetInterface的类
     * @param packageName 要扫描的包名（如 "com.example.service"）
     * @param targetInterface 目标接口
     */
    public static void autoInit(String packageName, Class targetInterface) {
        logger.info("自动初始化包：" + packageName + " 下实现 " + targetInterface.getSimpleName() + " 接口的类");
        try {
            // 1. 将包名转为文件路径（com.example → com/example）
            String packagePath = packageName.replace('.', '/');
            // 2. 获取类加载器
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            // 3. 获取所有匹配的资源（可能有多个，例如在不同的JAR文件中）
            Enumeration<URL> resources = classLoader.getResources(packagePath);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (resource == null) {
                    throw new RuntimeException("包路径不存在：" + packageName);
                }

                // 4. 根据资源类型选择不同的扫描方式
                String protocol = resource.getProtocol();
                if ("file".equals(protocol)) {
                    // 从文件系统扫描
                    File packageDir = new File(resource.toURI());
                    scanClassesFromFile(packageDir, packageName, targetInterface);
                } else if ("jar".equals(protocol)) {
                    // 从JAR文件扫描
                    scanClassesFromJar(resource, packagePath, packageName, targetInterface);
                }
            }

            // 4. 调用所有实例的初始化方法

        } catch (Exception e) {
            throw new RuntimeException("自动初始化失败", e);
        }
    }

    /**
     * 从文件系统递归扫描目录下的所有类文件
     */
    private static void scanClassesFromFile(File dir, String packageName, Class targetInterface)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        // 遍历目录下的文件/子目录
        for (File file : dir.listFiles()) {
            String fileName = file.getName();
            if (file.isDirectory()) {
                // 递归扫描子包
                scanClassesFromFile(file, packageName + "." + fileName, targetInterface);
            } else if (fileName.endsWith(".class") && !fileName.contains("$")) { // 排除内部类
                processClass(packageName, fileName, targetInterface);
            }
        }
    }

    /**
     * 从JAR文件扫描类文件
     */
    private static void scanClassesFromJar(URL jarUrl, String packagePath, String packageName, Class targetInterface)
            throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        URLConnection connection = jarUrl.openConnection();
        if (!(connection instanceof JarURLConnection)) {
            return;
        }

        JarURLConnection jarConnection = (JarURLConnection) connection;
        JarFile jarFile = jarConnection.getJarFile();

        // 遍历JAR文件中的所有条目
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();

            // 检查条目是否为类文件，并且属于目标包
            if (entryName.startsWith(packagePath) && entryName.endsWith(".class") && !entryName.contains("$")) {
                // 转换为类名
                String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
                // 加载类并初始化
                processClass(className, targetInterface);
            }
        }
    }

    /**
     * 处理类文件（从文件系统）
     */
    private static void processClass(String packageName, String fileName, Class targetInterface)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 解析类名（去掉 .class 后缀）
        String className = packageName + "." + fileName.substring(0, fileName.length() - 6);
        processClass(className, targetInterface);
    }

    /**
     * 处理类（通用方法）
     */
    private static void processClass(String className, Class targetInterface)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 加载类并校验类型
        Class<?> clazz = Class.forName(className);

        // 过滤条件：非抽象类 + 实现targetInterface + 有公共无参构造器
        if (!Modifier.isAbstract(clazz.getModifiers()) && targetInterface.isAssignableFrom(clazz)) {
            // 反射实例化（需保证子类有公共无参构造器）
            clazz.getDeclaredConstructor().newInstance();
        }
    }

    public static void main(String[] args) {
        // 示例：扫描并初始化指定包下的所有类
        String targetPackage = "com.zyc.push.pushx.action.wechat";
        System.out.println("Scanning package: " + targetPackage);

        //autoRegisterClasses(targetPackage, null);

        // 如果只想初始化实现特定接口的类
        // autoRegisterClasses(targetPackage, BaseAction.class);
    }
}