import h5py
import pandas as pd
from simpledbf import Dbf5
import re,csv
from tabulate import tabulate
import os
import subprocess
import configparser

config = configparser.ConfigParser()

# Read input parameters from control file
CTL_FILE                = os.environ.get('control_file')
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  os.environ.get('input_dir')
OUTPUT_FOLDER           =  os.environ.get('output_dir')


MS_SUMMIT_RPM9_CTL      = os.path.join(WORKING_FOLDER, config['mode_share']['MS_SUMMIT_RPM9_CTL'])
eqvfile                 = os.path.join(WORKING_FOLDER, config['mode_share']['MS_eqvfile'])
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
OUTPUT_FILENAME         = config['mode_share']['Modeshare_output_file_name']
MS_SUMMIT_CTL           = { 'RPM9':MS_SUMMIT_RPM9_CTL, 'champ':"MS_SUMMIT_CHAMP_CTL" }
MS_AUTO                 = 'Auto'
MS_TRANSIT              = 'Transit'
MS_PED                  = 'Pedestrian'
MS_BIKE                 = 'Bike'
MS_TNC                  =  "TNC"
MS_ROWS                 = { 'RPM9':[ MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC ],
                            'champ':[ MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC ] }


def readEqvFile(eqvfile):
	""" Reads the given eqvfile and returns
        distnames: distnum -> distname
		distToTaz: distnum -> list of taznums
		tazToDist: taznum  -> list of distnums
		numdists:  just the number of districts
	"""		
	f = open(eqvfile, 'r')
	eqvtxt = f.read()
	f.close()

	distline_re	= re.compile('DIST (\d+)=(\d+)( .+)?')
	lines 		= eqvtxt.split("\n")
	lineno 		= 0
	distnames	= {}
	distToTaz	= {}
	tazToDist	= {} 
	while (lineno < len(lines)):
		m 		= distline_re.search(lines[lineno])
		if (m != None):
			# distnames[int(m.group(1))] = m.group(2)
			dist= int(m.group(1))
			taz = int(m.group(2))
			if (dist not in distToTaz.keys()):
				distToTaz[dist] = [] 
			distToTaz[dist].append(taz)
			if (taz not in tazToDist.keys()):
				tazToDist[taz] = []
			tazToDist[taz].append(dist)
			if (m.group(3) != None):
				distnames[dist] = m.group(3).strip(' ')
		lineno	= lineno + 1
	numdists	= len(distnames)
	return (distnames, distToTaz, tazToDist, numdists)

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
        start = (key-1)*4+1
        for lineno in range(start,start+3):
            sumnums[key].append(lines[lineno].split("."))
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
def summitToModeSumToOrFrom(sumnums, numdists, runtype, within=True, timePeriod='Daily'):
    """ Normalizes summit output to Mode share numbers for to/from (and optionally within)
        runtype:    'champ' or 'RPM9'
        within:     True for To,From or Within; False for just To or From
            Returns
        timePeriod: "Daily", "AM", "MD", "PM", "EV", or "EA" (this is from the summit ctl)
        modesum: Auto|Transit|Pedestrian|Bike|Total -> list of trips for each dist for that mode
    """
    modesum = {}
    for mode in MS_ROWS[runtype]:
        modesum[mode] = []
    base = -1 
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
            # Auto, Auto toll, & Auto Paid
            modesum[MS_AUTO].append( \
                sumRowAndCol(sumnums[base+1],dist,dist,within, True) + \
                sumRowAndCol(sumnums[base+2],dist,dist,within, True) + \
                sumRowAndCol(sumnums[base+3],dist,dist,within, True))
            # Walk-To-Transit & Drivebase+-To-Transit
            modesum[MS_TRANSIT].append( \
                sumRowAndCol(sumnums[base+6],dist,dist,within, True) + \
                sumRowAndCol(sumnums[base+7],dist,dist,within, True))
            modesum[MS_PED].append( \
                sumRowAndCol(sumnums[base+4],dist,dist,within, True))
            modesum[MS_BIKE].append( \
                sumRowAndCol(sumnums[base+5],dist,dist,within, True))
            modesum['TNC'].append( \
                sumRowAndCol(sumnums[base+9],dist,dist,within, True) + \
                sumRowAndCol(sumnums[base+10],dist,dist,within, True))
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
    # print total
    return modeshare



# Check if the h5 file with the prefix PERSONTRIPS exists


# Check if the PERSONTRIPS_* h5 files exist
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
    #1-9 are Auto, Transit, Pedestrian, Bike tables
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



distnames, distToTaz, tazToDist, numdists = readEqvFile(eqvfile)

# reset as 0 index
downtown = [i-1 for i in distToTaz[1]]
rest_of_sf = [i-1 for i in distToTaz[2]]
rest_bay_area = [i-1 for i in distToTaz[3]]

#combine all the result into a large list
output = [ ]
for t in [t1,t2,t3,t4,t5,t6,t7,t8,t9,t10]:
    res = []
    for start in [downtown,rest_of_sf,rest_bay_area]:
        row = []
        for end in [downtown,rest_of_sf,rest_bay_area]:
            row.append(int(round(t[start][:,end].sum())))
        res.append(row)
    output.append(res)
    
table_list = [str(a)+str(b) for a in range(1,6) for b in [i for i in range(1,10)]+[20]]
for x in table_list:
    res = []
    for start in [downtown,rest_of_sf,rest_bay_area]:
        row = []
        for end in [downtown,rest_of_sf,rest_bay_area]:
            row.append(int(round(tables[f"ftable{x}"][start][:,end].sum())))
        res.append(row)
    output.append(res)

    
#generate the summary sum file like the "summit" application did
with open(f'{WORKING_FOLDER}/summit_file.sum', 'w') as file:
    file.write("\n")
    for res in output:
        for row in res:
            file.write('.'.join(map(str, row)) + '\n')
        file.write("\n")



tablekeys = [i for i in range(1,61)]


sumnums = readSummitSumFile(f'{WORKING_FOLDER}/summit_file.sum', tablekeys, numdists)

# Scale the summit numbers
sumnums = scaleSummitNums(sumnums)

# Define time periods of interest
timePeriods = ['Daily', 'AM', 'PM']

# Compute mode shares for each time period
res = {}
for time in timePeriods:
    # Convert summit data to mode sums and then to mode shares
    modesum = summitToModeSumToOrFrom(sumnums, numdists, 'RPM9', True, timePeriod=time)
    res[time] = modeSumToModeShare(modesum)

# Convert the mode shares to strings with a percent sign for printing to CSV
# for time in res.keys():
#     for key in res[time].keys():
#         res[time][key] = [f"{i}%" for i in res[time][key]]

# Define lists of places and transportation types of interest
places = ['Downtown', 'San Francisco', 'Bay Area']
types = [MS_AUTO, MS_TRANSIT, MS_PED, MS_BIKE, MS_TNC]

# Create a DataFrame for each time period and save to a CSV file

output_folder = OUTPUT_FOLDER
output_file   = OUTPUT_FILENAME 
for time in res.keys():
    df = pd.DataFrame(data=res[time], columns=types, index=places)
    df_melted = df.reset_index().melt(id_vars="index", var_name="mode", value_name="percentage")
    df_melted.columns = ["area", "mode", "percentage"]
    df_melted.to_csv(f"{output_folder}/{output_file}_{time}.csv", index=False)
    df = df.T
    df.index.name = 'Mode'
    formatted_df = df.copy()
    for col in formatted_df.columns:
        if pd.api.types.is_numeric_dtype(formatted_df[col]):
            formatted_df[col] = formatted_df[col].apply(lambda x: f"{x}%")
    markdown_table = tabulate(formatted_df, headers='keys', tablefmt='pipe').split('\n')
    markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**{"&nbsp;&nbsp;" if i > 0 else ""}' for i, header in enumerate(markdown_table[0].split('|')[1:-1])) + ' |'
    # markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
    alignment_row = markdown_table[1].split('|')[1:-1]
    for i, cell in enumerate(alignment_row):
        if i > 0:  # Skip the first column (header)
            alignment_row[i] = "---:"
    markdown_table[1] = '|' + '|'.join(alignment_row) + '|'
    markdown_table = '\n'.join(markdown_table)

    with open(f'{output_folder}/{output_file}_{time}.md', 'w') as f:
        f.write(markdown_table)
