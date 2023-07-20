import datetime,os,re,sys,subprocess
from socket         import gethostname
import pandas       as     pd
from tabulate import tabulate
import configparser




# Read input parameters from control file
CTL_FILE                            = os.environ.get('control_file')
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER                      =  os.environ.get('input_dir')
OUTPUT_FOLDER                       =  os.environ.get('output_dir')
necessary_scripts_for_vmt_folder    =  config['folder_setting']['necessary_scripts_for_vmt_folder']




# Access the arguments
final_file_paths = {
    'LOADAM_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['AM_vmt_vht_file']),
    'LOADPM_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['PM_vmt_vht_file']),
    'LOADMD_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['MD_vmt_vht_file']),
    'LOADEV_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['EV_vmt_vht_file']),
    'LOADEA_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['EA_vmt_vht_file'])
}

vmt_loadexport_cmd_path             = os.path.join(necessary_scripts_for_vmt_folder, config['vmt_vht']['VMT_LOADEXPORT_CMD'])
vmt_gawk_script_path                = os.path.join(necessary_scripts_for_vmt_folder, config['vmt_vht']['VMT_GAWK_SCRIPT'])
Output_filename                     = config['vmt_vht']['vmt_vht_output_file_name']
VMT_VMT                             = 'VMT'
VMT_VHT                             = 'VHT'
VMT_VMTOVERVHT                      = 'VMT/VHT'
VMT_LOSTTIME                        = 'Lost VH (vs freeflow)'
VMT_ROWS                            = [ VMT_VMT, VMT_VHT, VMT_VMTOVERVHT, VMT_LOSTTIME ]

def runtpp(scriptname, outputfile, env=None):
    """ Runs the given tpp script if the given outputfile doesn't exist
    """
    try:
        if (runAll):
            raise Exception('runAll', 'runAll')
        else:
            os.stat(outputfile)
    except:
        # it doesn't exist, run it
        hostname = gethostname()
        fullenv = None
        if env:
            fullenv = os.environ
            fullenv.update(env)
        # dispatch it, no cube licenseunCmd
        if (hostname == 'Mason' or hostname == 'D90V07K1-okforgetit'):
            f = open('topsheet.tmp', 'w')
            f.write("runtpp " + scriptname + "\n")
            f.close()
            scriptname = scriptname.replace('/', '\\')
            dproc    = subprocess.Popen( "Y:/champ/util/bin/dispatch.bat topsheet.tmp ocean", env=fullenv) 
            dret     = dproc.wait()
        else:
            dproc    = subprocess.Popen( "runtpp " + scriptname, env=fullenv)    
            dret     = dproc.wait()
        print ("dret = ",dret)
    return

def getVmtRaw(timePeriod):
    """ Returns vmt, vht, lost time for sf and rest of BA.
    """
    if (timePeriod=="Daily"):
        am_arr = getVmtRaw("AM")
        md_arr = getVmtRaw("MD")
        pm_arr = getVmtRaw("PM")
        ev_arr = getVmtRaw("EV")
        ea_arr = getVmtRaw("EA")
        daily_arr = []
        for ind in range(0,len(am_arr)):
            daily_arr.append(am_arr[ind] + md_arr[ind] + pm_arr[ind] + ev_arr[ind] + ea_arr[ind])
        return daily_arr

    loadfile= final_file_paths[f"LOAD{timePeriod}_FINAL.txt"]

    cmd     = "gawk -f " + vmt_gawk_script_path + " -v time=" +timePeriod \
                + " " + loadfile
    gawkproc= subprocess.run(cmd, shell=True, capture_output=True) 
    line    = gawkproc.stdout.decode().strip(' \r\n') 
    nums    = line.split(" ")
    return [ float(nums[0]), float(nums[1]), float(nums[2]), float(nums[8]), float(nums[9]), float(nums[10]) ]

def getVmt(timePeriod='Daily'):
    """ Returns standard format.  Just for sf and rest of ba tho. 
    """
    raw = getVmtRaw(timePeriod)
    vmt = {}
    vmt[VMT_VMT] = ( raw[0], raw[3] )
    vmt[VMT_VHT] = ( raw[1], raw[4] )
    vmt[VMT_VMTOVERVHT] = ( raw[0]/raw[1], raw[3]/raw[4] )
    vmt[VMT_LOSTTIME] = ( raw[2], raw[5] )
    return vmt

def format_with_commas(x):
    return '{:,.0f}'.format(x)


res = {'Daily':getVmt(),'AM':getVmt('AM'),'PM':getVmt("PM")}

output_folder = OUTPUT_FOLDER
output_file   = Output_filename
for timeperiod in res.keys():
    formatted_data = {}
    for key, value in res[timeperiod].items():
        if key == "VMT/VHT":
            formatted_data[key] = [str(round(v, 1)) for v in value]
        else:
            formatted_data[key] = tuple(format_with_commas(x) for x in value)
    df = pd.DataFrame(data = formatted_data,
                   index = ['San Francisco','Bay Area'])
    df.T.to_csv(f"{output_folder}/{output_file}_{timeperiod}.csv")
    vmt_vht = df['VMT/VHT']
    vmt_vht.to_csv(f"{output_folder}/{output_file}_{timeperiod}_ratio.csv", header=['value'], index=True, index_label='area')
    df = df.T
    df.index.name ='Type'
    markdown_table = tabulate(df, headers='keys', tablefmt='pipe', floatfmt='.0f').split('\n')
    markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
    markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
    markdown_table = '\n'.join(markdown_table)
    with open(f'{output_folder}/{output_file}_{timeperiod}.md', 'w') as f:
        f.write(markdown_table)