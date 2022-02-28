#!/usr/bin/sh
lsoffree=$(lsof | grep -E ".*.py$|spark.*py|test_spark.*py" | awk '{print $2}' | xargs echo kill -9 | bash); $lsoffree
clearvar=$(ps -ef | grep -E ".*.py$|park.*py|test_spark.*py" | grep -v "grep" | grep -v "restart" | awk '{print $2}' | xargs echo kill -9 | bash); $clearvar
