@echo off
setlocal enabledelayedexpansion

rem Получаем classpath
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt > nul
set /p CP=<cp.txt
del cp.txt

rem Запускаем приложение с необходимыми аргументами
java --add-opens=java.base/java.nio=ALL-UNNAMED ^
     --add-opens=java.base/java.lang=ALL-UNNAMED ^
     --add-opens=java.base/java.util=ALL-UNNAMED ^
     --add-opens=java.base/java.io=ALL-UNNAMED ^
     --add-opens=java.base/java.time=ALL-UNNAMED ^
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED ^
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
     --add-opens=java.base/sun.security.action=ALL-UNNAMED ^
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED ^
     --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED ^
     -cp "target/classes;!CP!" ^
     org.apache.ignite.cdc.CdcMain ^
     --consumer org.apache.ignite.cdc.CdcTester$TestCdcConsumer

endlocal