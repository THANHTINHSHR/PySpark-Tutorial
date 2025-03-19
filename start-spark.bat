@echo off
echo Starting Spark Master...
start /B C:\Spark\spark-3.5.5-bin-hadoop3\sbin\start-master.sh
timeout /t 5
echo Starting Spark Worker...
start /B C:\Spark\spark-3.5.5-bin-hadoop3\sbin\start-worker.sh spark://localhost:7077
echo Spark Started!
pause
