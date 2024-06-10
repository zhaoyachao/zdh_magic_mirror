set ff=unix
bin_path=$(cd `dirname $0`; pwd)
cd "$bin_path/.."
pt=`pwd`
nohup java -Dfile.encoding=utf-8 -Dloader.path=libs/,conf/ -Xms512M -jar "${pt}/"zdh_variable.jar >> variable.log  &