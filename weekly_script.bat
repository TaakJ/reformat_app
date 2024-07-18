@echo off

set params=%*
set default_dir=%cd%
set app_dir=D:\Document\Coding\Project\reformat_app
cd /d %app_dir%

call :main
goto exit

:function

:: Activate the virtual environment
REM call %cd%\.venv\Scripts\activate.bat

:: Main script
call uob_reformat %params%

:: Deactivate the virtual environment
REM call  %cd%\.venv\Scripts\deactivate.bat

cd /d %default_dir%

goto :eof

:main
call :function
goto :eof

:exit

exit /b "%errorlevel%"