set ff=unix
bin_path=`dirname "$0"`
cd "$bin_path/.."
pt=`pwd`
APP_NAME=${pt}"/zdh_label.jar"
nohup java -Dfile.encoding=utf-8 -Dloader.path=libs/,conf/ -Xss512M -jar "$APP_NAME"  >> label.log  &