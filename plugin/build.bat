@echo off
setlocal enabledelayedexpansion

REM 设置控制台编码为UTF-8，解决中文乱码问题
chcp 65001 >nul

REM 获取当前脚本所在目录
set "SCRIPT_DIR=%~dp0"

REM 获取模块名（当前目录名称）
for %%I in ("%SCRIPT_DIR%.") do set "MODULE=%%~nxI"

REM 切换到上层目录
cd /d "%SCRIPT_DIR%.." || (
    echo 错误：无法切换到上层目录
    pause
    exit /b 1
)

REM 显示当前路径和要构建的模块
echo 当前路径：%cd%
echo 构建模块：%MODULE%

REM 执行Maven构建命令
echo 正在执行构建命令：mvn clean package -pl "%MODULE%" -am -Dmaven.test.skip=true
call mvn clean package -pl "%MODULE%" -am -Dmaven.test.skip=true

REM 检查构建结果
if %ERRORLEVEL% equ 0 (
    echo 构建成功！
) else (
    echo 构建失败！
    pause
    exit /b 1
)

endlocal