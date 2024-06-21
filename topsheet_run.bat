@echo off
setlocal

set CHAMPVERSION=Y:\champ\dev\7.0.0_dev_25w
:: Path to the configuration file
set "CONFIG_FILE=%~dp0folders_config.txt"

:: Check if the configuration file exists
if not exist "%CONFIG_FILE%" (
    echo Configuration file not found: %CONFIG_FILE%
    exit /b 1
)

:: Load configurations from the config.txt file
for /f "tokens=1* delims==" %%a in (%CONFIG_FILE%) do (
    set "%%a=%%b"
)

:: Display the loaded configuration for verification
echo Working Folder: %WORKING_FOLDER%
echo Output Folder: %OUTPUT_FOLDER%
echo Scripts Folder: %SCRIPTS_FOLDER%

:: Change to the scripts directory
:: cd /d %SCRIPTS_FOLDER%

echo Running scripts in %SCRIPTS_FOLDER%...

:: Run each Python script
echo Running landuse
CALL python %SCRIPTS_FOLDER%\landuse.py
echo Running mode
CALL python %SCRIPTS_FOLDER%\mode.py
echo Running purpose, it will take about 30 minutes
CALL python %SCRIPTS_FOLDER%\purpose.py
echo Running vmt_vht
CALL python %SCRIPTS_FOLDER%\vmt_vht.py
echo Running transit
CALL python %SCRIPTS_FOLDER%\transit.py
echo Running traffic
CALL python %SCRIPTS_FOLDER%\traffic.py

endlocal

echo  scripts execution completed.
pause




