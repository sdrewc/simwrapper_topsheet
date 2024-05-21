@echo off
echo Running Python scripts...

:: Change directory to the folder containing the scripts
cd scripts

REM Create the temporary conda environment
CALL conda create --name topsheet_data -c conda-forge python=3.11 -y

REM Install pip in the environment
CALL conda run --name topsheet_data conda install pip -y

REM Install requirements using pip
CALL conda run --name topsheet_data python -m pip install -r requirements.txt

:: Run each Python script
CALL conda run --name topsheet_data python landuse.py
CALL conda run --name topsheet_data python mode.py
CALL conda run --name topsheet_data python rpt.py
CALL conda run --name topsheet_data python vmt_vht.py
CALL conda run --name topsheet_data python transit.py
CALL conda run --name topsheet_data python traffic.py

:: Pause for 10 seconds to ensure all file operations are completed
timeout /t 10 /nobreak

CALL conda env remove --name topsheet_data -y

echo Scripts execution completed.
pause



