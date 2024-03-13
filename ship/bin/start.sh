set ff=unix
bin_path=`dirname "$0"`
cd "$bin_path/.."
pt=`pwd`
APP_NAME=${pt}"/zdh_ship.jar"
nohup java -Dfile.encoding=utf-8 -Dloader.path=libs/,conf/ -Xms512M -jar "$APP_NAME"  >> ship.log  &