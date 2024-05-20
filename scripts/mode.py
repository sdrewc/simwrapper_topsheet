import h5py
import pandas as pd
import os
import subprocess
import configparser
from utilTools import readEqvFile, readCtlFile, DataFrameToCustomHTML,modifyDistrictNameForMap
from pathlib import Path



CTL_FILE                = r'../topsheet.ctl'
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  Path(config['folder_setting']['WORKING_FOLDER'])
OUTPUT_FOLDER           =  Path(config['folder_setting']['OUTPUT_FOLDER'])




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
rp_trips                = os.path.join(WORKING_FOLDER, 'daysim', 'abm_output1', config['mode_share']['RP_DISAG_TRIPS'])
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

def modeSumToModeShareSF(modesum):
    """ modesum numbers -> percentages for SF
    """
    modeshare   = {}
    for mode in modesum.keys():
        modeshare[mode] = sum(modesum[mode][:12])  
    tempSum = sum(modeshare.values())
    for mode in modesum.keys():
        modeshare[mode] = round(100 * modeshare[mode] / tempSum, 1)  
    return modeshare



def convert_mat_to_h5(directory, mat_file):
    # Construct the full paths to the input and output files
    input_file = f"{os.path.join(directory, mat_file)}.mat"
    executable = os.path.join(directory, 'mat2h5.exe')
    # Construct the command to run the conversion
    command = [executable, input_file]

    # Execute the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        print(f"Successfully converted {mat_file}.mat to {mat_file}.h5")
    else:
        print(f"Failed to convert {mat_file}.mat: {result.stderr}")

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
        mat_file_names = ['PERSONTRIPS_AM', 'PERSONTRIPS_EA', 'PERSONTRIPS_EV', 'PERSONTRIPS_MD', 'PERSONTRIPS_PM']
        for mat_file in mat_file_names:
            convert_mat_to_h5(WORKING_FOLDER, mat_file)


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

tablekeys = [i for i in range(1,61)]

distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)

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

sumnums = readSummitSumFile(os.path.join(WORKING_FOLDER, 'summit_file.sum'), tablekeys, numdists)

# Scale the summit numbers
sumnums = scaleSummitNums(sumnums)

# Define time periods of interest
timePeriods = ['Daily','AM','MD','PM','EV','EA']

# Compute mode shares for each time period
res = {}
sf = {}
for time in timePeriods:
    # Convert summit data to mode sums and then to mode shares
    # print(time)
    modesum = summitToModeSumToOrFrom(sumnums, numdists, 'RPM9', True, False, timePeriod=time)
    res[time] = modeSumToModeShare(modesum)
    sf[time] = modeSumToModeShareSF(modesum)

df = pd.DataFrame(sf).T
df.index.name = 'TOD'
df = df.reset_index()
df2html = DataFrameToCustomHTML([], [0])
df2html.generate_html(df, os.path.join(OUTPUT_FOLDER,'Mode_tod.md'), True)

df.to_csv(os.path.join(OUTPUT_FOLDER,'Mode_tod.csv'), index=False)


places = list(distnames.values())
types = [MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC]

df = pd.DataFrame(data=res['Daily'], columns=types, index=places)
df.index.name = 'District'
df = df.reset_index()
df_alt = df.loc[:,['District', MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC]]
df_alt.index.name = "District No"

df_alt = df_alt.reset_index()
df_alt['District No'] = df_alt['District No'] + 1
df2html = DataFrameToCustomHTML([], [0,1])
df2html.generate_html(df_alt, os.path.join(OUTPUT_FOLDER,'Mode_daily.md'), True)
df_alt.to_csv(os.path.join(OUTPUT_FOLDER,"Mode_daily.csv"),index=False)
df_alt = modifyDistrictNameForMap(df_alt, 'District')
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

def modeShareSF_tp(final_df, ocode, dcode, places, tp):
    # Filter the dataframe for the time period if it's not 'Daily'
    if tp != 'Daily':
        df = final_df[final_df['tp'] == tp]
    else:
        df = final_df

    # Check if ocode or dcode is in the list of places
    df = df[df[ocode].isin(places) | df[dcode].isin(places)]
    
    # Calculate sum of 'trexpfac' for different modes
    auto = df[df['mode'].isin([3, 4, 5])]['trexpfac'].sum()
    transit = df[df['mode'].isin([6])]['trexpfac'].sum()
    ped = df[df['mode'].isin([1])]['trexpfac'].sum()
    bike = df[df['mode'].isin([2])]['trexpfac'].sum()
    tnc = df[df['mode'].isin([9])]['trexpfac'].sum()
    
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
bins = [0, 180, 540, 780, 1080, 1440]  
labels = ['EA', 'AM', 'MD', 'PM', 'EV']

final_df['tp'] = pd.cut(final_df['deptm'], bins=bins, labels=labels, right=False)


distnames, distToTaz, tazToDist, numdists = readEqvFile(AREA_EQV)
final_df['o3code'] = final_df['otaz'].map(lambda x: tazToDist[x][0])
final_df['d3code'] = final_df['dtaz'].map(lambda x: tazToDist[x][0])

distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)
final_df['o15code'] = final_df['otaz'].map(lambda x: tazToDist[x][0])
final_df['d15code'] = final_df['dtaz'].map(lambda x: tazToDist[x][0])



timePeriods = ['Daily',  'AM', 'MD', 'PM', 'EV', 'EA']
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
df = df.loc[:,['District','Auto','Transit','Pedestrian','Bike','TNC']]

df = df.reset_index()
df2html = DataFrameToCustomHTML([], [0,1])
df2html.generate_html(df, os.path.join(OUTPUT_FOLDER,'Mode_tod_tab_district.md'), True)
df.to_csv(os.path.join(OUTPUT_FOLDER,'Mode_tod_tab_district.csv'),index=False)

df = modifyDistrictNameForMap(df, 'District')
df_alt.to_csv(os.path.join(OUTPUT_FOLDER,"district_mode_tod_tab_map.csv"),index=False)

sf = {}
for tp in timePeriods:
    percentage = modeShareSF_tp(final_df, 'o3code', 'd3code',[1,2], tp)
    tmp_sum = sum(percentage)
    for j in range(len(percentage)):
        percentage[j] = round(100 * percentage[j]/tmp_sum, 1)
    sf[tp] = percentage
df = pd.DataFrame(sf).T
df.columns = ['Auto', 'Transit','Pedestrian' , 'Bike','TNC']
df.index.name = 'TOD'
df = df.reset_index()

df2html = DataFrameToCustomHTML([], [0])
df2html.generate_html(df, os.path.join(OUTPUT_FOLDER,'Mode_tod_tab.md'), True)
df.to_csv(os.path.join(OUTPUT_FOLDER,'Mode_tod_tab.csv'),index=False)

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

os.remove(os.path.join(WORKING_FOLDER, 'summit_file.sum'))