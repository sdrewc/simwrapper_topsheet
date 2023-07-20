@echo off

::Input directory
set input_dir=C:\Users\wwang\Desktop\combine\test8

::Output directory
set output_dir=C:\Users\wwang\Desktop\combine\output_for_simwrapper

::Script directory
set script_dir=C:\Users\wwang\Desktop\combine\test10

set control_file=%script_dir%\topsheet.ctl

pushd %script_dir%

python topsheet_summary.py
pause

popd

echo DONE!!!
