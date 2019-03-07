#!/bin/bash

cd `dirname $0`
cd ..
project_dir=`pwd`
JAVA=$JAVA_HOME'/bin/java'
if [ -z $JAVA_HOME ]; then
 JAVA='java'
fi
conf_file=${project_dir}'/conf/collect.properties'

pidfile=${project_dir}/collector.pid
if [ -f $pidfile ];then
   ps -ef |grep `cat ${pidfile}` |grep -v 'grep'
   if [ $? -ne 0 ];then
      echo 'Another process is running, please stop it first!'
      exit 1
   fi
fi
nohup $JAVA -Dcollect.properties=$conf_file -cp  "${project_dir}/lib/*:${project_dir}/conf/"  io.sugo.collect.LogCollector > /dev/null  2>nohup.out &
if [ $? -eq 0 ];then
  echo $! > ${project_dir}/collector.pid
  echo 'PID:'$!' is started!'
fi
