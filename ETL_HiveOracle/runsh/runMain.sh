#!/bin/bash
cd /opt/workspace/$USER/notebooks/SOURCES_UPDATE/sourses/
./exitingRunningProcesses.sh > /dev/null 2>&1 && ./runAllScripts.sh


