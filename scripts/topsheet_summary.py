import os
import subprocess
if __name__ == '__main__':

    # required_packages = ['pandas==1.3.4', 'h5py==3.4.0', 'xlrd==2.0.1', 'numpy==1.21.4', 'tabulate==0.8.9', 'sqlalchemy==1.4.27', 'tables==3.6.1']

    # # Get a list of installed packages
    # installed_packages = subprocess.check_output(['pip', 'freeze']).decode()

    # # Check if each required package is installed
    # missing_packages = []
    # for package in required_packages:
    #     if package not in installed_packages:
    #         missing_packages.append(package)

    # Install missing packages
    # if missing_packages:
    #     subprocess.check_call(['pip', 'install'] + missing_packages)

    scripts = ['land_use.py', # Wei wrote this, ask him if any problems found
                'mode_share.py', # Wei wrote this, ask him if any problems found
                'transit.py', # Wei wrote this, ask him if any problems found
                'vmt_vht.py', # Wei wrote this, ask him if any problems found
                'rpt.py',# Madhav wrote this, ask him if any problems found
                'traffic.py' # Madhav wrote this, ask him if any problems found
                ]

    for script_name in scripts:
        print(f'Running {script_name}')

        # Run script depending on script name
        os.system(f'python {script_name}')

