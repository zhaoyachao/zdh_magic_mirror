package com.zyc.magic_mirror.common.http;

import javassist.Modifier;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

/**
 * Java包遍历和类初始化工具类
 */
public class PackageScanner {

    /**
     * 自动初始化指定包下所有继承 BaseAutoInit 的实现类
     * @param packageName 要扫描的包名（如 "com.example.service"）
     */
    public static void autoInit(String packageName, Class targetInterface) {
        try {
            // 1. 将包名转为文件路径（com.example → com/example）
            String packagePath = packageName.replace('.', '/');
            // 2. 获取类加载器，遍历包下的资源
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            URL resource = classLoader.getResource(packagePath);
            if (resource == null) {
                throw new RuntimeException("包路径不存在：" + packageName);
            }

            // 3. 遍历包下的所有 .class 文件
            File packageDir = new File(resource.toURI());
            scanClasses(packageDir, packageName, targetInterface);

            // 4. 调用所有实例的初始化方法

        } catch (Exception e) {
            throw new RuntimeException("自动初始化失败", e);
        }
    }

    /**
     * 递归扫描目录下的所有类文件
     */
    private static void scanClasses(File dir, String packageName, Class targetInterface) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        // 遍历目录下的文件/子目录
        for (File file : dir.listFiles()) {
            String fileName = file.getName();
            if (file.isDirectory()) {
                // 递归扫描子包
                scanClasses(file, packageName + "." + fileName, targetInterface);
            } else if (fileName.endsWith(".class") && !fileName.contains("$")) { // 排除内部类
                // 4. 解析类名（去掉 .class 后缀）
                String className = packageName + "." + fileName.substring(0, fileName.length() - 6);
                // 5. 加载类并校验类型
                Class<?> clazz = Class.forName(className);

                // 过滤条件：非抽象类 + 继承 BaseAutoInit + 有公共无参构造器
                if (!Modifier.isAbstract(clazz.getModifiers()) && targetInterface.isAssignableFrom(clazz)) {
                    // 6. 反射实例化（需保证子类有公共无参构造器）
                    clazz.getDeclaredConstructor().newInstance();
                }
            }
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