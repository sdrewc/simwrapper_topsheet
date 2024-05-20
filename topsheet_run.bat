@echo off
echo Running Python scripts...

:: Change directory to the folder containing the scripts
cd scripts

REM Create the temporary conda environment
CALL conda create --name topsheet -c conda-forge python=3.11 -y

REM Install pip in the environment
CALL conda run --name topsheet conda install pip -y

REM Install requirements using pip
CALL conda run --name topsheet python -m pip install -r requirements.txt

:: Run each Python script
CALL conda run --name topsheet python landuse.py
CALL conda run --name topsheet python mode.py
CALL conda run --name topsheet python rpt.py
CALL conda run --name topsheet python vmt_vht.py
CALL conda run --name topsheet python transit.py
CALL conda run --name topsheet python traffic.py

CALL conda env remove --name topsheet -y

echo Scripts execution completed.
pause



