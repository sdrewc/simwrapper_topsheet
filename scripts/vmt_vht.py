import pandas as pd
from tabulate import tabulate
import datetime,os,re,sys,subprocess
from socket         import gethostname
import configparser




# Read input parameters from control file
CTL_FILE                            = os.environ.get('control_file')
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER                      =  os.environ.get('input_dir')
OUTPUT_FOLDER                       =  os.environ.get('output_dir')




# Access the arguments
final_file_paths = {
    'LOADAM_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['AM_vmt_vht_file']),
    'LOADPM_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['PM_vmt_vht_file']),
    'LOADMD_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['MD_vmt_vht_file']),
    'LOADEV_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['EV_vmt_vht_file']),
    'LOADEA_FINAL.txt': os.path.join(WORKING_FOLDER ,config['vmt_vht']['EA_vmt_vht_file'])
}

Output_filename                     = config['vmt_vht']['vmt_vht_output_file_name']
VMT_VMT                             = 'VMT'
VMT_VHT                             = 'VHT'
VMT_VMTOVERVHT                      = 'VMT/VHT'
VMT_LOSTTIME                        = 'Lost VH (vs freeflow)'
VMT_ROWS                            = [ VMT_VMT, VMT_VHT, VMT_VMTOVERVHT, VMT_LOSTTIME ]
def getVmtRaw( timePeriod):
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
    
    time_factors = {"AM": 0.44, "MD": 0.18, "PM": 0.37, "EV": 0.22, "EA": 0.58}
    file = "LOAD" + timePeriod + "_FINAL.csv"
    df = pd.read_csv(file)
    df = df.loc[df['FT'] != 6]
    peakHourFactor = time_factors.get(timePeriod)
    df.loc[df['SPEED'] == 0, 'time_freeflow'] = 0
    df.loc[df['SPEED'] != 0, 'time_freeflow'] = (df['DISTANCE'] / df['SPEED']) * 60
    df['lost_time'] = ((df['TIME_1'] - df['time_freeflow']) / 60) * df['V_1']
    vmt_sf = df.loc[df['MTYPE'] == 'SF', ['DISTANCE', 'V_1']].prod(axis=1).sum()
    vht_sf = df.loc[df['MTYPE'] == 'SF', ['TIME_1', 'V_1']].prod(axis=1).sum()/60
    losttime_sf = df.loc[df['MTYPE'] == 'SF', 'lost_time'].sum()
    vmt_region = df[['DISTANCE', 'V_1']].prod(axis=1).sum()
    vht_region = df[['TIME_1', 'V_1']].prod(axis=1).sum()/60
    losttime_region = df['lost_time'].sum()
    vmt_region_losf = df.loc[df['V_1']*peakHourFactor/(df['CAP']*df['LANE_AM']+0.00001)>1,['DISTANCE', 'V_1']].prod(axis=1).sum()
    filtered_df = df.loc[df['MTYPE'] == 'SF']
    selected_columns = ['DISTANCE', 'V_1']
    filtered_columns = filtered_df[selected_columns]
    vmt_sf_losf = filtered_df.loc[filtered_df['V_1']*peakHourFactor/(filtered_df['CAP']*filtered_df['LANE_AM']+0.00001)>1,['DISTANCE', 'V_1']].prod(axis=1).sum()
    vmt_nonsf = vmt_region - vmt_sf
    vht_nonsf = vht_region - vht_sf
    losttime_nonsf = losttime_region - losttime_sf
    vmt_nonsf_losf = vmt_region_losf - vmt_sf_losf
    return [vmt_sf, vht_sf, losttime_sf,vmt_region,vht_region, losttime_region]


def getVmt(timePeriod='Daily'):
    """ Returns standard format.  Just for sf and rest of ba tho. 
    """
    raw = getVmtRaw(timePeriod)
    vmt = {}
    vmt['VMT'] = ( raw[0], raw[3] - raw[0],raw[3] )
    vmt['VHT'] = ( raw[1], raw[4] -raw[1], raw[4] )
    vmt['Speed'] = ( raw[0]/raw[1],(raw[3] - raw[0])/(raw[4] -raw[1]), raw[3]/raw[4] )
    vmt['Lost VH (vs freeflow)'] = ( raw[2], raw[5] -raw[2], raw[5] )
    return vmt

def format_with_commas(x):
    return '{:,.0f}'.format(x)




def dictToMD(res,res_index, index_name,  index_list, output_file): 
    formatted_data = {'VMT':[],'VHT':[],'Speed':[],'Lost VH (vs freeflow)':[]}
    for tp in index_list:
        for key, value in res[tp].items():
            if key == "Speed":
                formatted_data[key].append(round(value[res_index], 1))
            else:
                formatted_data[key].append(format_with_commas(round(value[0])))
    df = pd.DataFrame(data = formatted_data,
                   index = index_list)
    df = df.T
    df['TOD'] = df.index
    df[index_name] = df.index
    df = df[[index_name]+[col for col in df if col != index_name]]
    bold_headers = ["<b>" + col + "</b>" for col in df.columns]
    md_table = tabulate(df, headers=bold_headers,showindex=False, tablefmt='pipe')
    with open(f'{output_file}.md', 'w') as f:
        f.write(md_table)




def getVC(region,output_file, index_name):
    res = {'Drive Alone':[],'Shared Ride 2':[],'Shared Ride 3+':[], 'Trucks':[], 'Commercial Vehicles':[], 'TNC':[]}
    for file in ['LOADAM_FINAL.csv','LOADMD_FINAL.csv','LOADPM_FINAL.csv','LOADEV_FINAL.csv','LOADEA_FINAL.csv']:
        df = pd.read_csv(file)
        df = df.loc[df['FT'] != 6]
        if region == 'sf':
            df = df.loc[df['MTYPE'] == 'SF']
        elif region =='nonsf':
            df = df.loc[df['MTYPE'] != 'SF']
        elif region == 'ba':
            pass
        res['Drive Alone'].append(df[['DISTANCE', 'V1_1']].prod(axis=1).sum() + df[['DISTANCE', 'V4_1']].prod(axis=1).sum() + df[['DISTANCE', 'V7_1']].prod(axis=1).sum())
        res['Shared Ride 2'].append(df[['DISTANCE', 'V2_1']].prod(axis=1).sum() + df[['DISTANCE', 'V5_1']].prod(axis=1).sum() + df[['DISTANCE', 'V8_1']].prod(axis=1).sum())
        res['Shared Ride 3+'].append(df[['DISTANCE', 'V3_1']].prod(axis=1).sum() + df[['DISTANCE', 'V6_1']].prod(axis=1).sum() + df[['DISTANCE', 'V9_1']].prod(axis=1).sum())
        res['Trucks'].append(df[['DISTANCE', 'V10_1']].prod(axis=1).sum() + df[['DISTANCE', 'V11_1']].prod(axis=1).sum() + df[['DISTANCE', 'V12_1']].prod(axis=1).sum())
        res['Commercial Vehicles'].append(df[['DISTANCE', 'V13_1']].prod(axis=1).sum() + df[['DISTANCE', 'V14_1']].prod(axis=1).sum() + df[['DISTANCE', 'V15_1']].prod(axis=1).sum())
        res['TNC'].append(df[['DISTANCE', 'V16_1']].prod(axis=1).sum() + df[['DISTANCE', 'V17_1']].prod(axis=1).sum() + df[['DISTANCE', 'V18_1']].prod(axis=1).sum())
    df = pd.DataFrame(data = res,
           index = ['AM','MD','PM', 'EV', 'EA'])
    df =df.T
    class_sums = df.sum()


    for column in df.columns:
        df[column] = round(df[column] / class_sums[column],4)
    df_melt = df.reset_index().melt(id_vars='index', var_name='Time', value_name='Percentage')
    df_melt.columns = ['Category', 'Time', 'Percentage']
    df_melt.to_csv(f'{output_file}.csv',index=False)
    for key, values in res.items():
        tmp = []
        for value in values:
            tmp.append(format_with_commas(round(value)))
        res[key] = tmp
        
    df = pd.DataFrame(data = res,
           index = ['AM','MD','PM', 'EV', 'EA'])
    df = df.T
    df['TOD'] = df.index
    df[index_name] = df.index
    df = df[[index_name]+[col for col in df if col != index_name]]
    bold_headers = ["<b>" + col + "</b>" for col in df.columns]
    md_table = tabulate(df, headers=bold_headers,showindex=False, tablefmt='pipe')

    with open(f'{output_file}.md', 'w') as f:
        f.write(md_table)

res = {'Daily':getVmt(),'AM':getVmt('AM'),'PM':getVmt("PM"), 'MD':getVmt('MD'), 'EV': getVmt('EV'), 'EA': getVmt('EA')}


timeperiod = 'Daily'
formatted_data = {}
for key, value in res[timeperiod].items():
    if key == "Speed":
        formatted_data[key] = [str(round(v, 1)) for v in value]
    else:
        formatted_data[key] = tuple(format_with_commas(x) for x in value)
df = pd.DataFrame(data = formatted_data,
               index = ['SF','Rest of Bay Area','Bay Area'])

df = df.T

df['Geography'] = df.index
df = df[['Geography'] + [col for col in df if col != 'Geography']]
df.columns = ['Geography', 'SF', 'Rest of Bay Area   ', 'Bay Area']
bold_headers = ["<b>" + col + "</b>" for col in df.columns]

md_table = tabulate(df, headers=bold_headers,showindex=False, tablefmt='pipe')
with open(f'vmt_{timeperiod}.md', 'w') as f:
    f.write(md_table)

dictToMD(res, 0, 'TOD', ['AM','MD','PM', 'EV', 'EA'], 'vmt_sf' )
dictToMD(res, 1, 'TOD', ['AM','MD','PM', 'EV', 'EA'], 'vmt_nonsf' )
dictToMD(res, 2, 'TOD', ['AM','MD','PM', 'EV', 'EA'], 'vmt_ba' )
getVC('sf','vmt_vc_sf','TOD')
getVC('nonsf','vmt_vc_nonsf','TOD')
getVC('ba','vmt_vc_ba','TOD')