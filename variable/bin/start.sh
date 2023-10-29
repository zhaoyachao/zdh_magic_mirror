set ff=unix
bin_path=`dirname "$0"`
cd "$bin_path/.."
pt=`pwd`
nohup java -Dfile.encoding=utf-8 -Dloader.path=libs/,conf/ -Xss512M -jar "${pt}/"zdh_variable.jar >> variable.log  &