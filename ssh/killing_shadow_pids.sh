#!/usr/bin/sh
rm -r -f /opt/yarn/nm/usercache/$USER/filecache/*
ls -lrt /tmp/* 2>/dev/null | grep 'krb' | grep $USER | awk '{print $9}' | xargs rm -f | bash
/bin/ps -u $USER| grep -E "python|jupyter" | grep -v "grep" | grep -v "restart" | awk '{print $1}' | xargs echo kill -9 | bash
PASS=$(cat /home/$(whoami)/pass/userpswrd | sed 's/\r//g'); echo $PASS | kinit

