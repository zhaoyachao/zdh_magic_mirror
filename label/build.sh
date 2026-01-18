#!/bin/bash

# 获取当前脚本所在目录
SCRIPT_DIR="$(dirname "$0")"

# 获取模块名（当前目录名称）
MODULE="$(basename "$SCRIPT_DIR")"

# 切换到上层目录
cd "$SCRIPT_DIR/.." || {
    echo "错误：无法切换到上层目录"
    exit 1
}

# 显示当前路径和要构建的模块
echo "当前路径：$(pwd)"
echo "构建模块：$MODULE"

# 执行Maven构建命令
echo "正在执行构建命令：mvn clean package -pl \"$MODULE\" -am -Dmaven.test.skip=true"
mvn clean package -pl "$MODULE" -am -Dmaven.test.skip=true

# 检查构建结果
if [ $? -eq 0 ]; then
    echo "构建成功！"
else
    echo "构建失败！"
    exit 1
fi