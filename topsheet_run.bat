@echo off
setlocal

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
cd /d %SCRIPTS_FOLDER%

:: Check if the Conda environment exists
FOR /F "tokens=*" %%i IN ('conda info --envs ^| findstr /C:"topsheet_data"') DO SET ENV_EXISTS=%%i

:: Create the environment if it does not exist
IF NOT DEFINED ENV_EXISTS (
    CALL conda create --name topsheet_data -c conda-forge python=3.11 -y
    CALL conda run --name topsheet_data conda install pip -y
    CALL conda run --name topsheet_data python -m pip install -r %CONFIG_FILE%\requirements.txt
) ELSE (
    ECHO Environment already exists.
)

:: Example of script execution (Assume running a Python script as an example)
echo Running scripts in %SCRIPTS_FOLDER%...

:: Run each Python script
echo Running landuse
CALL conda run --name topsheet_data python landuse.py
echo Running mode
CALL conda run --name topsheet_data python mode.py
echo Running rpt, it will take about 30 minutes
CALL conda run --name topsheet_data python rpt.py
echo Running vmt_vht
CALL conda run --name topsheet_data python vmt_vht.py
echo Running transit
CALL conda run --name topsheet_data python transit.py
echo Running traffic
CALL conda run --name topsheet_data python traffic.py

endlocal

echo  scripts execution completed.
pause




