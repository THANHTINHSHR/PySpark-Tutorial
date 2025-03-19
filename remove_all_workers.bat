@echo off
cd C:\Spark\spark-3.5.5-bin-hadoop3\sbin
call stop-workers.sh
timeout /t 3
rmdir /s /q C:\Spark\spark-3.5.5-bin-hadoop3\work
echo "All workers removed!"