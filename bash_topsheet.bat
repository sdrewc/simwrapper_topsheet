::@echo off
::"Path where your Python exe is stored\python.exe" "Path where your Python script is stored\script ::name.py"
::pause
@echo off

::Run directory
set rundir=C:\Users\MPonnudurai\Documents\Mad\Simwrapper\Topsheet\final

::Output directory
set outputdir=C:\Users\MPonnudurai\Documents\Mad\Simwrapper\Topsheet\Result

::Config directory
set configdir=C:\Users\MPonnudurai\Documents\Mad\Simwrapper\Topsheet

set control_file=%configdir%\ctl_topsheet.py

:: py scripts dir -- needed??


cd \d %rundir%

echo Running Python script...
python rpt.py %configdir% %outputdir% %control_file%


echo Done!!.