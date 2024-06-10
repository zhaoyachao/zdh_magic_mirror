set ff=unix
bin_path=$(cd `dirname $0`; pwd)
cd "$bin_path/.."
pt=`pwd`
kill -15 `ps -ef |grep "${pt}/"zdh_variable.jar |grep java|awk -F " " '{print $2}'`