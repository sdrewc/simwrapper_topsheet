import h5py
import pandas as pd
from simpledbf import Dbf5
import re,csv
from tabulate import tabulate
import os
import subprocess
import configparser
import numpy as np
from utilTools import readEqvFile, readCtlFile



# Set working and output directories from environment variables
WORKING_FOLDER          =  'input/modeshare'
OUTPUT_FOLDER           =  'output'

# Retrieve the control file path from environment and read its contents
# This script requires the following files from the control file: 
# "tazdata.dbf", "modesumSimple3_RPM9.ctl", and "DIST15.eqv"

CTL_FILE                = 'topsheet.ctl'

config = configparser.ConfigParser()
config.read(CTL_FILE)



MS_SUMMIT_RPM9_CTL      = os.path.join(WORKING_FOLDER, config['mode_share']['MS_SUMMIT_RPM9_CTL'])
DIST_EQV                = os.path.join(WORKING_FOLDER, config['mode_share']['DIST_EQV'])
AREA_EQV                = os.path.join(WORKING_FOLDER, config['mode_share']['AREA_EQV'])
AM_h5_file              = os.path.join(WORKING_FOLDER, config['mode_share']['AM_h5_file'])
MD_h5_file              = os.path.join(WORKING_FOLDER, config['mode_share']['MD_h5_file'])
PM_h5_file              = os.path.join(WORKING_FOLDER, config['mode_share']['PM_h5_file'])
EV_h5_file              = os.path.join(WORKING_FOLDER, config['mode_share']['EV_h5_file'])
EA_h5_file              = os.path.join(WORKING_FOLDER, config['mode_share']['EA_h5_file'])
AM_mat_file             = os.path.join(WORKING_FOLDER, config['mode_share']['AM_mat_file'])
MD_mat_file             = os.path.join(WORKING_FOLDER, config['mode_share']['MD_mat_file'])
PM_mat_file             = os.path.join(WORKING_FOLDER, config['mode_share']['PM_mat_file'])
EV_mat_file             = os.path.join(WORKING_FOLDER, config['mode_share']['EV_mat_file'])
EA_mat_file             = os.path.join(WORKING_FOLDER, config['mode_share']['EA_mat_file'])
Convert_mat2h5          = os.path.join(WORKING_FOLDER, config['mode_share']['Convert_mat2h5'])
rp_trips                = os.path.join(WORKING_FOLDER, config['mode_share']['RP_DISAG_TRIPS'])
OUTPUT_FILENAME         = config['mode_share']['Modeshare_output_file_name']
MS_SUMMIT_CTL           = { 'RPM9':MS_SUMMIT_RPM9_CTL, 'champ':"MS_SUMMIT_CHAMP_CTL" }
MS_AUTO                 = 'Auto'
MS_TRANSIT              = 'Transit'
MS_PED                  = 'Pedestrian'
MS_BIKE                 = 'Bike'
MS_TNC                  =  "TNC"
MS_ROWS                 = { 'RPM9':[ MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC ],
                            'champ':[ MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC ] }

def readSummitSumFile(sumfile, tablekeys, numdists):
    """ Reads the given summit sumfile and returns
    sumnums: tablekey -> 2d list of number strings (numdist x numdist)
    """
    f 			= open(sumfile)
    sumtxt 		= f.read()
    f.close()

    lines		= sumtxt.split("\n")
    sumnums		= {}
    
    for key in tablekeys:
        sumnums[key] = []
        start = (key-1)*(numdists+1)+1
        # print(start)
        for lineno in range(start,start+numdists):
            sumnums[key].append(lines[lineno].split("|"))
    return sumnums

def scaleSummitNums(sumnums):
	""" Scale the unscaled and make everything a number.  Return floats.
	"""
	for key in sumnums.keys():
		for i1 in range(0,len(sumnums[key])):
			for i2 in range(0,len(sumnums[key][i1])):
				sumnums[key][i1][i2] = float(sumnums[key][i1][i2])

	return sumnums

def sumRowAndCol(squarelist, row, col, includeIntersection=True, cumulative=True):
    """ What it sounds like
    """
    sum = 0
    if (cumulative == False):
        intersection = squarelist[row][col] 
        for r in range(0, len(squarelist)):
            sum = sum + squarelist[r][col]
        for c in range(0, len(squarelist)):
            sum = sum + squarelist[row][c]
        # we double counted this one
        sum = sum - intersection
        if (includeIntersection == False):
            sum = sum - intersection
    else:
        sum = 0
        # rows first
        for r in range(0, row+1):
            for c in range(0, len(squarelist)):
                sum = sum + squarelist[r][c]
        # cols
        for c in range(0, col+1):
            for r in range(row+1, len(squarelist)):
                sum = sum + squarelist[r][c]
    return sum      
def summitToModeSumToOrFrom(sumnums, numdists, runtype, within=True, culmulative = True, timePeriod='Daily'):
    """ Normalizes summit output to Mode share numbers for to/from (and optionally within)
        runtype:    'champ' or 'RPM9'
        within:     True for To,From or Within; True for just To or From
            Returns
        timePeriod: "Daily", "AM", "MD", "PM", "EV", or "EA" (this is from the summit ctl)
        modesum: Auto|Transit|Pedestrian|Bike|Total -> list of trips for each dist for that mode
    """
    modesum = {}
    for mode in MS_ROWS[runtype]:
        modesum[mode] = []
    base = -1 
    # base = 0
    if (timePeriod=='Daily'):   base = 0
    elif (timePeriod=='AM'):    base = 10
    elif (timePeriod=='MD'):    base = 20
    elif (timePeriod=='PM'):    base = 30
    elif (timePeriod=='EV'):    base = 40
    elif (timePeriod=='EA'):    base = 50
    else: 
        print( "Don't understand timePeriod: "+timePeriod)
        return modesum
    if (runtype == "RPM9"):
        for dist in range(0, numdists): 
            # TO NOte we have changed the cummulative as False to not have a cummulative sum for districts
            # Auto, Auto toll, & Auto Paid
            modesum[MS_AUTO].append( \
                sumRowAndCol(sumnums[base+1],dist,dist,within, culmulative) + \
                sumRowAndCol(sumnums[base+2],dist,dist,within, culmulative) + \
                sumRowAndCol(sumnums[base+3],dist,dist,within, culmulative))
            # Walk-To-Transit & Drivebase+-To-Transit
            modesum[MS_TRANSIT].append( \
                sumRowAndCol(sumnums[base+6],dist,dist,within, culmulative) + \
                sumRowAndCol(sumnums[base+7],dist,dist,within, culmulative))
            modesum[MS_PED].append( \
                sumRowAndCol(sumnums[base+4],dist,dist,within, culmulative))
            modesum[MS_BIKE].append( \
                sumRowAndCol(sumnums[base+5],dist,dist,within, culmulative))
            modesum['TNC'].append( \
                sumRowAndCol(sumnums[base+9],dist,dist,within, culmulative) + \
                sumRowAndCol(sumnums[base+10],dist,dist,within, culmulative))
    elif (runtype == "champ"):
        for dist in range(0, numdists): 
            modesum[MS_AUTO].append( \
                sumRowAndCol(sumnums[base+1],dist,dist,within, True)) 
            modesum[MS_TRANSIT].append( \
                sumRowAndCol(sumnums[base+2],dist,dist,within, True))
            modesum[MS_PED].append( \
                sumRowAndCol(sumnums[base+3],dist,dist,within, True))
            modesum[MS_BIKE].append( \
                sumRowAndCol(sumnums[base+4],dist,dist,within, True))
    else:
        print ("Could not calculate mode share for unknown runtype " + runtype)
    return modesum

def modeSumToModeShare(modesum):
    """ modesum numbers -> percentages for each dist
    """
    total       = []
    modeshare   = {}
    for mode in modesum.keys():
        modeshare[mode] = []    
    for dist in range(0,len(modesum[mode])):
        total.append(0)
        for mode in modesum.keys():
            modeshare[mode].append(0.0)
            total[dist] = total[dist] + modesum[mode][dist]
        for mode in modesum.keys():
            num = round(100 * float(modesum[mode][dist]) / float(total[dist]),1)
            modeshare[mode][dist] = num
    return modeshare
def extract_second_element(x):
    if isinstance(x, list) and len(x) >= 2:
        return x[1]
    return None


h5_files = [AM_h5_file, PM_h5_file, MD_h5_file, EV_h5_file, EA_h5_file]
missing_h5_files = []
for h5_file in h5_files:
    if not os.path.exists(h5_file):
        missing_h5_files.append(h5_file)
if not missing_h5_files:
    print("h5 files with prefix PERSONTRIPS exist")
else:
    # Check if the corresponding MAT files exist
    mat_files = [AM_mat_file, PM_mat_file, MD_mat_file, EV_mat_file, EA_mat_file]
    missing_mat_files = []
    for mat_file in mat_files:
        if not os.path.exists(mat_file):
            missing_mat_files.append(mat_file)
    if missing_mat_files:
        print(f"MAT files with prefix PERSONTRIPS missing: {missing_mat_files}")
    else:
        # Convert MAT files to h5 files using mat2h5.exe
        for mat_file in mat_files:
            # Set the name of the input MAT file
            input_mat_file = mat_file
            
            # Set the name of the output H5 file
            output_h5_file = os.path.splitext(mat_file)[0] + '.h5'
            output_h5      = os.path.join(WORKING_FOLDER,output_h5_file)
            # Run the mat2h5.exe file with the input and output filenames as arguments
            subprocess.run([Convert_mat2h5, input_mat_file, output_h5])


ftables = {"ftable1": AM_h5_file,
           "ftable2": MD_h5_file,
           "ftable3": PM_h5_file,
           "ftable4": EV_h5_file,
           "ftable5": EA_h5_file}
tables = {}
# store all tables in the "tables" dictionary 
for ftable, filename in ftables.items():
    with h5py.File(filename, "r") as f:
        for name in f.keys():
            table = f[name][...] # get all the dataset
            # so the first table in "ftable1 would be ftable11"
            tables[ftable + name] = table

for i in range(1, 6):
    #1-8 are Auto, Transit, Pedestrian, Bike tables
    for j in range(1, 9):
        name = "t{}{}".format(i, j)
        value = tables["ftable{}{}".format(i, j)]
        #store all tables in the local dictionary, 
        # so the variable t11 would be the first table in "ftable1"
        locals()[name] = value
    #19, 20 are TNC tables
    for j in range(19,21):
        name = "t{}{}".format(i, j)
        value = tables["ftable{}{}".format(i, j)]
        locals()[name] = value

#sum them up   
t1 = t11 + t21 + t31 + t41 + t51
t2 = t12 + t22 + t32 + t42 + t52
t3 = t13 + t23 + t33 + t43 + t53
t4 = t14 + t24 + t34 + t44 + t54
t5 = t15 + t25 + t35 + t45 + t55
t6 = t16 + t26 + t36 + t46 + t56
t7 = t17 + t27 + t37 + t47 + t57
t8 = t18 + t28 + t38 + t48 + t58
t9 = t119 + t219 + t319 + t419 + t519
t10 = t120 + t220 + t320 + t420 + t520

distnames, distToTaz, tazToDist, numdists = readEqvFile(AREA_EQV)


distnameToTaz = {}
for i in range(1,numdists+1):
    distnameToTaz[distnames[i]] = [j-1 for j in distToTaz[i]]
output = []
for t in [t1,t2,t3,t4,t5,t6,t7,t8,t9,t10]:
    res = []
    for start in list(distnameToTaz.values()):
        row = []
        for end in list(distnameToTaz.values()):
            row.append(t[start][:,end].sum())
        res.append(row)
    output.append(res)
table_list = [str(a)+str(b) for a in range(1,6) for b in [i for i in range(1,9)]+[19, 20]]
for x in table_list:
    res = []
    for start in list(distnameToTaz.values()):
        row = []
        for end in list(distnameToTaz.values()):
            row.append(tables[f"ftable{x}"][start][:,end].sum())
        res.append(row)
    output.append(res)

with open(os.path.join(WORKING_FOLDER, 'summit_file.sum'), 'w') as file:
    file.write("\n")
    for res in output:
        for row in res:
            file.write('|'.join(map(str, row)) + '\n')
        file.write("\n")



tablekeys = [i for i in range(1,61)]


sumnums = readSummitSumFile(os.path.join(WORKING_FOLDER, 'summit_file.sum'), tablekeys, numdists)

# Scale the summit numbers
sumnums = scaleSummitNums(sumnums)

# Define time periods of interest
timePeriods = ['Daily','AM','MD','PM','EV','EA']

# Compute mode shares for each time period
res = {}
tmp = {}
modesum_ck={}
percentage_dict = dict()
for time in timePeriods:
    # Convert summit data to mode sums and then to mode shares
    # print(time)
    modesum = summitToModeSumToOrFrom(sumnums, numdists, 'RPM9', True, True,timePeriod=time)
    tmp[time] = modesum
    res[time] = modeSumToModeShare(modesum)
    modesum_ck[time] = modesum
    total_mode_sum = dict()
    for key in modesum.keys():
        total_mode_sum[key]=np.sum(modesum[key][0:12])
    sumt=0
    for val in total_mode_sum.values():
        sumt+=val
    
    percentage_dict[time] = {key: round((value / sumt) * 100,2) for key, value in total_mode_sum.items()}
    # percentage_dict[time] = {key: value for key, value in total_mode_sum.items()}

# Convert the mode shares to strings with a percent sign for printing to CSV
# for time in res.keys():
#     for key in res[time].keys():
#         res[time][key] = [f"{i}%" for i in res[time][key]]

# Define lists of places and transportation types of interest
places = list(distnames.values())
types = [MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC]

df = pd.DataFrame(data=res).T
df_second_element = df.apply(lambda x: x.map(extract_second_element))


df_second_element = df_second_element.reset_index()

df_second_element.to_csv(os.path.join(OUTPUT_FOLDER,'Mode_tod.csv'),index=False)

distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)

sumnums = readSummitSumFile(f'summit_file.sum', tablekeys, numdists)

# Scale the summit numbers
sumnums = scaleSummitNums(sumnums)

# Define time periods of interest
timePeriods = ['Daily','AM','MD','PM','EV','EA']

# Compute mode shares for each time period
res = {}
tmp = {}
modesum_ck={}
percentage_dict = dict()
for time in timePeriods:
    # Convert summit data to mode sums and then to mode shares
    # print(time)
    modesum = summitToModeSumToOrFrom(sumnums, numdists, 'RPM9', True, False, timePeriod=time)
    tmp[time] = modesum
    res[time] = modeSumToModeShare(modesum)
    modesum_ck[time] = modesum
    total_mode_sum = dict()
    for key in modesum.keys():
        total_mode_sum[key]=np.sum(modesum[key][0:12])
    sumt=0
    for val in total_mode_sum.values():
        sumt+=val
    
    percentage_dict[time] = {key: round((value / sumt) * 100,2) for key, value in total_mode_sum.items()}
places = list(distnames.values())
types = [MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC]

df = pd.DataFrame(data=res['Daily'], columns=types, index=places)
df.index.name = 'District'
df = df.reset_index()
df_alt = df.loc[:,['District','Auto','Transit','TNC','Bike','Pedestrian']]
df_alt.index.name = "District No"
markdown_table = tabulate(df_alt, headers='keys', tablefmt='pipe').split('\n')
markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
markdown_table = '\n'.join(markdown_table)
with open(os.path.join(OUTPUT_FOLDER,'Mode_dist_daily.md'), 'w') as f:
    f.write(markdown_table)

df_alt.to_csv(os.path.join(OUTPUT_FOLDER,"Mode_daily.csv"),index=False)

df_alt.at[2, 'District'] = 'N.Beach/'
df_alt.at[3, 'District'] = 'Western'
df_alt.at[4, 'District'] = 'Mission/'
df_alt.at[5, 'District'] = 'Noe/'
df_alt.at[6, 'District'] = 'Marina/'
df_alt.at[9, 'District'] = 'Outer'
df_alt.at[10, 'District'] = 'Hill'

df_alt.to_csv(os.path.join(OUTPUT_FOLDER,"district_mode_tod.csv"),index=False)

comb_arr=[]
for j in range(1,numdists+1):
    comb_arr.append([i-1 for i in distToTaz[j]])
flatten_comb_arr = [item for sublist in comb_arr for item in sublist]
output = [ ]
for t in [t1,t2,t3,t4,t5,t6,t7,t8,t9,t10]:
    res = []
    for taz in flatten_comb_arr:
        tmp = sumRowAndCol(t,taz,taz,True, False)
        res.append(tmp)
    output.append(res)

taz_df = pd.DataFrame(data=output,index=range(1,11),columns=flatten_comb_arr)
taz_df=taz_df.T

T_taz_df = taz_df.reset_index(names='Taz')
mode_taz = pd.DataFrame()
mode_taz['Auto'] = T_taz_df.iloc[:,1:4].sum(axis=1)
mode_taz['Transit'] = T_taz_df.iloc[:,6:8].sum(axis=1)
mode_taz['Pedestrian'] = T_taz_df.iloc[:,4:5].sum(axis=1)
mode_taz['Bike'] = T_taz_df.iloc[:,5:6].sum(axis=1)
mode_taz['TNC'] = T_taz_df.iloc[:,9:11].sum(axis=1)

mode_taz = mode_taz.apply(lambda col: 100*col/sum(col),axis=1)
mode_taz['Taz'] = T_taz_df['Taz']

mode_taz.to_csv(os.path.join(OUTPUT_FOLDER,'taz_mode.csv'),index=False)



#--------------------------------------------------------------------------------------------------
def modeShare_tp(final_df, ocode, dcode, place, tp):
    if tp != 'Daily':
        df = final_df[final_df['tp'] == tp]
    else:
        df =final_df
    auto = df[((df[ocode] == place) | (df[dcode] == place)) & (df['mode'].isin([3,4,5]))]['trexpfac'].sum()
    transit = df[((df[ocode] == place) | (df[dcode] == place)) & (df['mode'].isin([6]))]['trexpfac'].sum()
    ped = df[((df[ocode] == place) | (df[dcode] == place)) & (df['mode'].isin([1]))]['trexpfac'].sum()
    bike = df[((df[ocode] == place) | (df[dcode] == place)) & (df['mode'].isin([2]))]['trexpfac'].sum()
    tnc = df[((df[ocode] == place) | (df[dcode] == place)) & (df['mode'].isin([9]))]['trexpfac'].sum()
    return [auto, transit, ped, bike, tnc]

def get_mode_shares(place, mode_sums):
    relevant_rows = mode_sums[(mode_sums['otaz'] == place) | (mode_sums['dtaz'] == place)]
    mode_shares = {
        3: 0, # Auto
        4: 0, # Could merge with auto if they are the same
        5: 0, # Same as above
        6: 0, # Transit
        1: 0, # Ped
        2: 0, # Bike
        9: 0  # TNC
    }
    for mode in mode_shares.keys():
        mode_shares[mode] = relevant_rows[relevant_rows['mode'] == mode]['trexpfac'].sum()
    return [mode_shares[mode] for mode in sorted(mode_shares)]

necessary_columns = ['dpurp', 'tour_id', 'tseg', 'deptm', 'endacttm', 'otaz', 'mode', 'dtaz','trexpfac']
df = pd.read_csv(rp_trips, delimiter='\t', usecols=necessary_columns)

df['original_index'] = df.index
df_temp = df[df['dpurp'] == 10].copy()
df_temp['tseg'] += 1
merged_df = pd.merge(df, df_temp, left_on=['tour_id', 'tseg', 'deptm'], right_on=['tour_id', 'tseg', 'endacttm'], suffixes=('', '_next'),  indicator=True)
merged_df['deptm'] = merged_df['deptm_next']
merged_df['otaz'] = merged_df['otaz_next']
merged_df['mode'] = 6  

indices_to_delete = []
indices_to_delete.extend(list(merged_df['original_index']))
indices_to_delete.extend(list(merged_df['original_index_next']))
cols_to_drop = [col for col in merged_df.columns if col.endswith('_next')]
merged_df.drop(cols_to_drop, axis=1, inplace=True)
df_cleaned = df.drop(indices_to_delete)
final_df = pd.concat([merged_df, df_cleaned], ignore_index=True)
distnames, distToTaz, tazToDist, numdists = readEqvFile(AREA_EQV)

final_df['o3code'] = final_df['otaz'].map(lambda x: tazToDist[x][0])
final_df['d3code'] = final_df['dtaz'].map(lambda x: tazToDist[x][0])
bins = [0, 180, 540, 780, 1080, 1440]  
labels = ['EA', 'AM', 'MD', 'PM', 'EV']

final_df['tp'] = pd.cut(final_df['deptm'], bins=bins, labels=labels, right=False)

timePeriods = ['Daily',  'AM', 'MD', 'PM', 'EV', 'EA']
sf = {}
for tp in timePeriods:
    tmp = [0] * 5
    for pc in [1,2]:
        percentage = modeShare_tp(final_df, 'o3code', 'd3code',pc, tp)
        for i in range(len(tmp)):
            tmp[i] += percentage[i]
    tmp_sum = sum(tmp)
    for j in range(len(tmp)):
        tmp[j] = round(100 * tmp[j]/tmp_sum, 1)
    sf[tp] = tmp

df = pd.DataFrame(sf).T
df.columns = ['Auto', 'Transit', 'TNC' , 'Bike','Pedestrian']
df.index.name = 'TOD'
markdown_table = tabulate(df, headers='keys', tablefmt='pipe').split('\n')
markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
markdown_table = '\n'.join(markdown_table)
with open(f'Mode_tod_tab.md', 'w') as f:
    f.write(markdown_table)
df = df.reset_index()
df.to_csv(os.path.join(OUTPUT_FOLDER,"Mode_tod_tab.csv"),index=False)

distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)
final_df['o15code'] = final_df['otaz'].map(lambda x: tazToDist[x][0])
final_df['d15code'] = final_df['dtaz'].map(lambda x: tazToDist[x][0])

timePeriods = ['Daily', 'EA', 'AM', 'MD', 'PM', 'EV']
districts = {}

for pc in distnames.keys():
    percentage = modeShare_tp(final_df, 'o15code', 'd15code',pc, 'Daily')
    tmp_sum = sum(percentage)
    for j in range(len(percentage)):
        percentage[j] = round(100 * percentage[j]/tmp_sum, 1)
    districts[pc] = percentage

df = pd.DataFrame(districts).T
df.columns = ['Auto', 'Transit', 'Pedestrian', 'Bike', 'TNC']
df.index.name = 'District NO'
df['District'] = df.apply(lambda row: distnames[row.name] , axis=1)
df = df.loc[:,['District','Auto','Transit','TNC','Bike','Pedestrian']]
markdown_table = tabulate(df, headers='keys', tablefmt='pipe').split('\n')
markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
markdown_table = '\n'.join(markdown_table)

with open(f'Mode_tod_tab_district.md', 'w') as f:
    f.write(markdown_table)
df.to_csv(os.path.join(OUTPUT_FOLDER,"district_mode_tod_tab.csv"),index=False)
df_alt = df
df_alt.at[3, 'District'] = 'N.Beach/'
df_alt.at[4, 'District'] = 'Western'
df_alt.at[5, 'District'] = 'Mission/'
df_alt.at[6, 'District'] = 'Noe/'
df_alt.at[7, 'District'] = 'Marina/'
df_alt.at[10, 'District'] = 'Outer'
df_alt.at[11, 'District'] = 'Hill'
df_alt.to_csv(os.path.join(OUTPUT_FOLDER,"district_mode_tod_tab_map.csv"),index=False)

comb_arr=[]
for j in range(1,numdists+1):
    comb_arr.append([i-1 for i in distToTaz[j]])
flatten_comb_arr = [item for sublist in comb_arr for item in sublist]

mode_sums = final_df.groupby(['otaz', 'dtaz', 'mode'])['trexpfac'].sum().reset_index()

output = [get_mode_shares(taz, mode_sums) for taz in flatten_comb_arr]

final = []
for i in range(len(output)):
    res = []
    auto = output[i][0]+output[i][1]+output[i][2]
    res.append(auto)
    res.append(output[i][3])
    res.append(output[i][4])
    res.append(output[i][5])
    res.append(output[i][6])
    res.append(flatten_comb_arr[i])
    final.append(res)
taz_df = pd.DataFrame(data=final)
row_sums = taz_df.iloc[:, :-1].sum(axis=1)
for col in taz_df.columns[:-1]:  # Exclude the last column
    taz_df[col] = 100 * taz_df[col] / row_sums
taz_df.columns = ['Auto',	'Transit',	'Pedestrian',	'Bike',	'TNC',	'Taz']
taz_df.to_csv(os.path.join(OUTPUT_FOLDER,'taz_mode_tab.csv'),index=False)