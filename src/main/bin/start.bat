@echo off  
echo path:%~dp0  
cd ..
set base=%~dp0 
set project_dir=%~dp0..
set conf=%project_dir%\conf
set lib=%project_dir%\lib
set conf_file=%project_dir%\conf\collect.properties

echo project_dir:%project_dir%

echo conf:%conf%
echo lib:%lib%
echo conf_file:%conf_file%

set pidfile=%project_dir%\collector.pid

echo pidfile:%pidfile%
if exist "%pidfile%" (
	echo "Another process is running, please stop it first!"
	for /f %%a in ('type %pidfile%') do (
		set pid=%%a
	) 

	echo pid:%pid%
) else (
	echo not exist
)
	

IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%/bin/java"
)

JAVA -Dcollect.properties=%conf_file% -cp  %lib%\*;%conf%\*  io.sugo.collect.LogCollector 

@pause  