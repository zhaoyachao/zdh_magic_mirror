set "binDir=%~dp0"
set "baseDir=%binDir:~0,-1%"
pushd "%baseDir%" && cd ..
java -Xss512M -jar zdh_label.jar