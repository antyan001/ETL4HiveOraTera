#!/usr/bin/sh

port=8100

process_id=`/bin/ps -u $USER| grep "jupyter" | grep -v "grep" | grep -v "restart" | awk '{print $1}' | xargs echo kill -9 | bash`
$process_id

 #echo 'Killing'
 #for pid in $process_id
 #do
 #    echo "KILL: $pid"
 #    kill -9 $pid
 #    sleep 1
 #done

echo 'Starting JL:'
rm -f nohup.out
nohup jupyter_notebook35 --notebook-dir=/opt/workspace/$USER/notebooks --ip 0.0.0.0 --port $port &

# sleep 2

# token=`jupyter-notebook35 list | grep 'token=' | sed -r 's/.*token=(\w+) .*/\1/'`
# ns=`hostname`
# echo 'JL link:'
# echo http://$ns:$port/?token=$token
