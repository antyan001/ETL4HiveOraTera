#!/usr/bin/sh

sheduler=1990
crit_limit=120

PASS=$(cat /home/$(whoami)/pass/userpswrd | sed 's/\r//g'); echo $PASS | kinit
# kinit -kt /home/$(whoami)/keytab/user.keytab $(whoami)@DF.SBRF.RU
cd /opt/workspace/$(whoami)/notebooks/AUTOKILLER/

# if [[ "$#" -lt 1 ]]; then

#   echo "Usage: $0  <max_life_in_mins>"

#   exit 1

# fi

yarn application -list 2>/dev/null | grep -v "report" | grep -v "Total" | grep "RUNNING" | awk '{print $1}' > job_list.txt

for jobId in `cat job_list.txt`

do

finish_time=`yarn application -status $jobId 2>/dev/null | grep "Finish-Time" | awk '{print $NF}'`

if [[ $finish_time -ne 0 ]]; then

  echo "App $jobId is not running"

  exit 1

fi

compl_jobs=`curl https://pklis-chd00${sheduler}.labiac.df.sbrf.ru:8090/proxy/redirect/$jobId 2>/dev/null | tr -d '\n' | tr -s ' ' | grep -Po "(?<=Completed Jobs\:\<\/strong\>\<\/a\>)[0-9 ]*(?=\<\/li\>)" | sed 's/[[:space:]]*//g'`

echo "Completed jobs for $jobId: $compl_jobs"

active_jobs=`curl https://pklis-chd00${sheduler}.labiac.df.sbrf.ru:8090/proxy/redirect/$jobId 2>/dev/null | tr -d '\n' | tr -s ' ' | grep -Po "(?<=Active Jobs\:\<\/strong\>\<\/a\>)[0-9 ]*(?=\<\/li\>)" | sed 's/[[:space:]]*//g'`

echo "Active jobs for $jobId: $active_jobs"

last_running_job=`curl https://pklis-chd00${sheduler}.labiac.df.sbrf.ru:8090/proxy/redirect/$jobId 2>/dev/null | tr -d '\n' | tr -s ' ' | grep -P -o -m 1 "object running.*?Submitted\:\K([0-9:/ ]*)(?:\w+)" | sed 's/[[:space:]]*//g'`
last_succeeded_job=`curl https://pklis-chd00${sheduler}.labiac.df.sbrf.ru:8090/proxy/redirect/$jobId 2>/dev/null | tr -d '\n' | tr -s ' ' | grep -P -o -m 1 "object succeeded.*?Completed\:\K([0-9:/ ]*)(?:\w+)" | sed 's/[[:space:]]*//g'`

echo "Last Running DT: ${last_running_job:0:10} ${last_running_job:10:8}"
echo "Last Succeeded DT: ${last_succeeded_job:0:10} ${last_succeeded_job:10:8}"

if [[ (-z $last_running_job) ]]; then

durat_last_running_job_=-60

else

running_dt=`date +'%s' -d "${last_running_job:0:10} ${last_running_job:10:8}"`
durat_last_running_job_=`date +%s`-$running_dt

fi

if [[ (-z $last_succeeded_job) ]]; then

durat_last_succeeded_job_=-60

else

running_dt=`date +'%s' -d "${last_succeeded_job:0:10} ${last_succeeded_job:10:8}"`
durat_last_succeeded_job_=`date +%s`-$running_dt

fi

time_diff_in_mins_running=`echo "("$durat_last_running_job_")/60" | bc`
time_diff_in_mins_succeeded=`echo "("$durat_last_succeeded_job_")/60" | bc`

echo "Last Running Difference for $jobId: $time_diff_in_mins_running"
echo "Last Succeeded Difference for $jobId: $time_diff_in_mins_succeeded"

time_diff=`date +%s`-`yarn application -status $jobId 2>/dev/null | grep "Start-Time" | awk '{print $NF}' | sed 's!$!/1000!'`
time_diff_in_mins=`echo "("$time_diff")/60" | bc`
echo "App $jobId is running for $time_diff_in_mins min(s)"

if [[ ($time_diff_in_mins_succeeded -gt ${crit_limit}) && ($time_diff_in_mins_running -eq -1) ]]; then

  echo "Killing app $jobId"

  yarn application -kill $jobId

else

  echo "App $jobId should continue to run"

fi

done
