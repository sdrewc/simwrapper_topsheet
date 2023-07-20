import datetime,os,re,sys,subprocess
import xlrd,xlwt        # excel reader and writer
import pandas as pd
from simpledbf import Dbf5
from tabulate import tabulate
import csv
import os
import simpledbf
import configparser




# Read input parameters from control file
CTL_FILE                = os.environ.get('control_file')
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  os.environ.get('input_dir')
OUTPUT_FOLDER           =  os.environ.get('output_dir')


AM_csv                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAAM_CSV'])
PM_csv                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAPM_CSV'])
EA_csv                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEA_CSV'])
MD_csv                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAMD_CSV'])
EV_csv                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEV_CSV'])
AM_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAAM_DBF'])
PM_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAPM_DBF'])
MD_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAMD_DBF'])
EV_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEV_DBF'])
EA_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEA_DBF'])
OUTPUT_FILENAME         =  config['transit']['Transit_File_Name']
linked_muni_files = {
    'AM': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_AM_DBF']),
    'PM': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_PM_DBF']),
    'MD': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_MD_DBF']),
    'EV': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_EV_DBF']),
    'EA': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_EA_DBF'])
}

TIMEPERIODS =  { 1:"EA", 2:"AM", 3:"MD", 4:"PM", 5:"EV" }
TRN_QUICKBOARDS_CTL     = 'quickboards-topsheet.ctl'  # use local - it might have nodes.xls configured
TRN_QUICKBOARDS_OUT     = 'topsheet-quickboards.xls'
TRN_LINESTATS_SHEET     = 'LineStats'
TRN_LINEDETAIL_SHEET    = 'Line Detail'
TRN_MUNI_RAIL           = 'All Rail'
TRN_MUNI_BUS            = 'All Bus'
TRN_MUNI_TOTAL          = 'Total'
TRN_MUNI_GEARY          = 'Geary Corridor(38 & 38L)'
TRN_MUNI_VN             = 'Van Ness (47 & 49)'
TRN_MUNI_MISSION        = 'Mission (49 & 14)' 
# TRN_LINKED_SCRIPT       = SCRIPT_PATH + "countLinkedMuniTrips.s"
# Don't have to run this script
TRN_LINKED_OUT          = "LINKEDMUNI_%TOD%.DBF"
TRN_XFER_LINKED         = '~Linked'
TRN_XFER_RATE           = '~Xfer Rate'
TRN_MUNI_ROWS           = [ 'Muni Boardings', [TRN_MUNI_GEARY,1], [TRN_MUNI_VN,1], [TRN_MUNI_MISSION,1], 
                            TRN_MUNI_RAIL, TRN_MUNI_BUS, TRN_MUNI_TOTAL, [TRN_XFER_LINKED,1], [TRN_XFER_RATE,1] ]

TRN_BART_TUBE           = 'BART (Transbay Tube)'
TRN_BART_SM             = 'BART (SM County Line)'
TRN_CALTRAIN_SM         = 'Caltrain (SM County Line)'
TRN_RAIL_VOL_ROWS       = [ 'Rail Volumes', TRN_BART_TUBE, TRN_BART_SM, TRN_CALTRAIN_SM ]
TRN_BART                = 'BART'
TRN_CALTRAIN            = 'Caltrain'
TRN_RAIL_BRD_ROWS       = [ 'Rail Boardings', TRN_BART, TRN_CALTRAIN ]
TRN_ACTRANSIT           = 'AC Transit (Bay Bridge)'
TRN_GGT                 = 'Golden Gate Transit (GG Bridge)'
TRN_SAMTRANS            = 'SamTrans (SM County Line)'
TRN_BUS_ROWS            = [ 'Bus Volumes', TRN_ACTRANSIT, TRN_GGT, TRN_SAMTRANS ]
BART_STOPS_DOWNTOWN     = [ 16511, # Embarcadero
                            16512, # Montgomery
                            16513, # Powell
                            16514, # Civic Center
                            # for BART new tube extension
                            15350, # Folsom & Fremont
                            15351, # Folsom & 4th
                            15352 ]# Civic Center 
BART_STOPS_SF           = [ 16515, # 16th/Mission
                            16516, # 24th/Mission
                            16517, # Glen Park
                            16518, # Balboa Park
                            # for BART new tube extension
                            15353, # Geary & VN
                            15354, # Geary & Fillmore
                            15355, # Geary & Masonic/Presidio 
                            15356  # Presidio
                          ]
BART_STOPS_SF.extend(BART_STOPS_DOWNTOWN)

CALTRAIN_STOPS_DOWNTOWN = [ 14654,   # Transbay Terminal 
                            14655 ]  # 4th and King
CALTRAIN_STOPS_SF       = [ 14656, # 22nd Street 
                            14653, # Oakdale Ave (unders study)
                            14657, # Paul Ave (before 2005)
                            14658  # Bayshore
                          ]
CALTRAIN_STOPS_SF.extend(CALTRAIN_STOPS_DOWNTOWN)

def getTransitBoardingsForLine(line,df,stops=[]):
    linelen     = len(line)
    boardings    = 0
    if (len(stops)==0):
        tmp = df.query(f'NAME.str.startswith("{line}")', engine='python')
        boardings += tmp['AB_BRDA'].sum()
    else:
        tmp = df.query(f'NAME.str.startswith("{line}")', engine='python')
        tmp_stop = tmp.query(f"A in{stops}")
        boardings += sum(tmp_stop['AB_BRDA'])
    return boardings

class SanFranciscoNodeChecker:
    """
    The following query works well enough to create a bounding box for San Francisco
    in GIS (Create a new selection on a FREEFLOW_nodes.shp file).  The second bit is to include
    Treasure Island without the southern tip of Marin.
    
    (Y > 2086000 And Y < 2129000 And X < 6024550) Or 
    (Y > 2086000 And Y < 2140000 And X > 6019000 And X < 6024550) 
    """
    
    # static variable
    NodesToCoords = None
    
    def __init__(self):
        if not SanFranciscoNodeChecker.NodesToCoords:
            SanFranciscoNodeChecker.NodesToCoords = {}
            freenodes = os.path.join(WORKING_FOLDER, config['transit']['FREEFLOW_nodes_DBF'])
            dbf = Dbf5(freenodes)
            df = dbf.to_dataframe()
            for index, row in df.iterrows():
                N = row['N']
                X = row['X']
                Y = row['Y']
                SanFranciscoNodeChecker.NodesToCoords[N] = (X, Y)

    def inSF(self, nodenum):
        try:
            coords = SanFranciscoNodeChecker.NodesToCoords[nodenum]
        except:
            return False
        if coords[0] < 6024550 and coords[1] > 2086000 and coords[1] < 2129000:
            return True
        
        # treasure island
        if coords[0] > 6019000 and coords[0] < 6024550 and coords[1] > 2086000 and coords[1] < 2140000:
            return True
        
        return False
    
def getTransitVolumes(system, df):
    sfNC = SanFranciscoNodeChecker()
    sf          = [] 
    nonsf       = [] 
    if (system==TRN_BART_TUBE):
        # rail lines have their own nodes
        prefixes= [ "100_" ]
        sf      = BART_STOPS_SF
        nonsf   = [ 16510, # W Oakland
                    16534 ]# Lake Merrit
    elif (system==TRN_BART_SM):
        prefixes= [ "100_" ]
        sf      = BART_STOPS_SF
        nonsf   = [ 16519 ]    # Daly city
    elif (system==TRN_CALTRAIN_SM):
        prefixes= ["101_",
                   "CALT" ]
        sf       = CALTRAIN_STOPS_SF
        nonsf    = [14659,    # S SF 
                    14660,    # San Bruno
                    14661,    # Millbrae
                    14662,    # Broadway
                    14663,    # Burlingame
                    14664,    # San Mateo
                    14665,    # Hayward Park
                    14667,    # Hillsdale
                  ]
    elif (system == TRN_GGT):
        prefixes = ["80_" ]
    elif (system == TRN_ACTRANSIT):
        prefixes = ["37_", "40_"]
    elif (system == TRN_SAMTRANS):
        prefixes = ["26_", "27_", "28_", "30_" ]
    else:
        print ("Do not understand system " + system)
        return [0,0]
    prefixlen     = len(prefixes[0])
    tmp = df[df["NAME"].str.startswith(tuple(prefixes))]
    if len(sf) ==0 :
        outbound = tmp[tmp["A"].map(lambda x:sfNC.inSF(x))& ~ tmp["B"].map(lambda x:sfNC.inSF(x))]['AB_VOL'].sum()
        inbound = tmp[tmp["B"].map(lambda x:sfNC.inSF(x))& ~ tmp["A"].map(lambda x:sfNC.inSF(x))]['AB_VOL'].sum()
        return [inbound,outbound] 
    else:
        outbound = tmp[tmp["A"].map(lambda x: x in sf ) & tmp["B"].map(lambda x: x in nonsf)]['AB_VOL'].sum()
        inbound = tmp[tmp["B"].map(lambda x: x in sf ) & tmp["A"].map(lambda x: x in nonsf)]['AB_VOL'].sum()
        return [inbound, outbound]

def getTransitBoardings(df,timePeriod):
    transit     = {}
    transit[TRN_MUNI_BUS]   = [ 0 ]
    transit[TRN_MUNI_RAIL]  = [ 0 ]
    transit[TRN_MUNI_TOTAL] = [ 0 ]
    tmp = df.query(f'NAME.str.startswith("MUN")', engine='python')
    bus_endings = ['CPX','HPX','NX','TISH']
    df_bus_sliced1 = tmp[tmp["NAME"].str.slice(3,-1).isin(bus_endings)]
    transit[TRN_MUNI_BUS] += df_bus_sliced1['AB_BRDA'].sum()
    rail_endings = ['59','60','61']
    df_rail_sliced1 = tmp[tmp["NAME"].str.slice(3,5).isin(rail_endings)]
    transit[TRN_MUNI_RAIL] += df_rail_sliced1['AB_BRDA'].sum()
    df_bus_sliced2 = tmp[tmp['NAME'].str.slice(3,4).str.isnumeric() & ~tmp['NAME'].str.slice(3,5).isin(["59","60","61"])]
    transit[TRN_MUNI_BUS] += df_bus_sliced2['AB_BRDA'].sum()
    df_rail_sliced2 = tmp[~tmp.index.isin(df_bus_sliced1.index) 
                          & ~tmp.index.isin(df_rail_sliced1.index) & ~tmp.index.isin(df_bus_sliced2.index)]
    transit[TRN_MUNI_RAIL] += df_rail_sliced2['AB_BRDA'].sum()
    transit[TRN_MUNI_TOTAL][0] = transit[TRN_MUNI_RAIL][0] + transit[TRN_MUNI_BUS][0]
    
    
    
    transit[TRN_MUNI_GEARY] = [ \
    getTransitBoardingsForLine( "MUN38", df) ]
    transit[TRN_MUNI_VN] = [ \
    getTransitBoardingsForLine( "MUN47",df) + \
    getTransitBoardingsForLine( "MUN49", df) ]
    transit[TRN_MUNI_MISSION] = [ \
    getTransitBoardingsForLine("MUN49", df) + \
    getTransitBoardingsForLine( "MUN14", df) ]

    transit[TRN_BART] = [ \
    getTransitBoardingsForLine("100_",df, BART_STOPS_DOWNTOWN) , \
    getTransitBoardingsForLine("100_",df, BART_STOPS_SF) , \
    getTransitBoardingsForLine( "100_", df) ]
    transit[TRN_CALTRAIN] = [ \
    getTransitBoardingsForLine("101_", df, CALTRAIN_STOPS_DOWNTOWN) , \
    getTransitBoardingsForLine("101_", df, CALTRAIN_STOPS_SF) , \
    getTransitBoardingsForLine("101_", df) ]
    if (transit[TRN_CALTRAIN][0] == 0):
        transit[TRN_CALTRAIN] = [ \
            getTransitBoardingsForLine("CALTRN", df, CALTRAIN_STOPS_DOWNTOWN) , \
            getTransitBoardingsForLine("CALTRN",df, CALTRAIN_STOPS_SF) , \
            getTransitBoardingsForLine( "CALTRN", df) ]
    transit[TRN_BART_TUBE] = \
    getTransitVolumes( TRN_BART_TUBE, df)
    transit[TRN_BART_SM] = \
    getTransitVolumes( TRN_BART_SM, df)
    transit[TRN_CALTRAIN_SM] = \
    getTransitVolumes( TRN_CALTRAIN_SM, df)
    transit[TRN_ACTRANSIT] = \
    getTransitVolumes(TRN_ACTRANSIT, df)
    transit[TRN_GGT] = \
    getTransitVolumes(TRN_GGT, df)
    transit[TRN_SAMTRANS] = \
    getTransitVolumes( TRN_SAMTRANS, df)
        # estimate linked trips
    transit[TRN_XFER_LINKED] = [ 0 ]


    tp =  timePeriod 

    
    # outfile = TRN_LINKED_OUT
    # outfile = outfile.replace("%TOD%",tp)

    if os.path.exists(linked_muni_files[tp]):    
        # these are already attempting to just count trips with muni
        dbf = Dbf5(linked_muni_files[tp])
        df = dbf.to_dataframe()
        for index, rec in df.iterrows():
                # skip non-sf to non-sf
                if rec['ORIG'] == 3 and rec['DEST'] == 3: continue
                transit[TRN_XFER_LINKED][0] += rec['WLOC']
                transit[TRN_XFER_LINKED][0] += rec['WLRT']
                transit[TRN_XFER_LINKED][0] += rec['WPRE']
                transit[TRN_XFER_LINKED][0] += rec['WFER']
                transit[TRN_XFER_LINKED][0] += rec['WBAR']
                transit[TRN_XFER_LINKED][0] += rec['DLOCW']
                transit[TRN_XFER_LINKED][0] += rec['DLRTW']
                transit[TRN_XFER_LINKED][0] += rec['DPREW']
                transit[TRN_XFER_LINKED][0] += rec['DFERW']
                transit[TRN_XFER_LINKED][0] += rec['DBARW']                    
                transit[TRN_XFER_LINKED][0] += rec['WLOCD']
                transit[TRN_XFER_LINKED][0] += rec['WLRTD']
                transit[TRN_XFER_LINKED][0] += rec['WPRED']
                transit[TRN_XFER_LINKED][0] += rec['WFERD']
                transit[TRN_XFER_LINKED][0] += rec['WBARD']
    return transit

def get_total_transit(res_dict):
    transit_daily = {}
    transit_daily[TRN_MUNI_BUS] = [0]
    transit_daily[TRN_MUNI_RAIL] = [0]
    transit_daily[TRN_MUNI_TOTAL] = [ 0 ]
    transit_daily[TRN_MUNI_GEARY] =[0]
    transit_daily[TRN_MUNI_VN] = [0]
    transit_daily[TRN_MUNI_MISSION] =[0]
    transit_daily[TRN_BART] =[0,0,0]
    transit_daily[TRN_CALTRAIN] = [0,0,0]
    transit_daily[TRN_BART_TUBE] = [0,0]
    transit_daily[TRN_BART_SM] = [0,0]
    transit_daily[TRN_CALTRAIN_SM] = [0,0]
    transit_daily[TRN_GGT] =[0,0]
    transit_daily[TRN_ACTRANSIT]=[0,0]
    transit_daily[TRN_SAMTRANS] =[0,0]
    transit_daily[TRN_XFER_LINKED] =[0]
    for k,v in res_dict.items():
        transit_daily[TRN_MUNI_BUS][0] += v[TRN_MUNI_BUS][0]
        transit_daily[TRN_MUNI_RAIL][0] += v[TRN_MUNI_RAIL][0]
        transit_daily[TRN_MUNI_TOTAL][0] += v[TRN_MUNI_TOTAL][ 0 ]
        transit_daily[TRN_MUNI_GEARY][0] += v[TRN_MUNI_GEARY][0]
        transit_daily[TRN_MUNI_VN][0] += v[TRN_MUNI_VN][0]
        transit_daily[TRN_MUNI_MISSION][0] +=v[TRN_MUNI_MISSION][0]
        for i, val in enumerate(v[TRN_BART]):
            transit_daily[TRN_BART][i] += val
        for i, val in enumerate(v[TRN_CALTRAIN]):
            transit_daily[TRN_CALTRAIN][i] += val
        for i, val in enumerate(v[TRN_BART_TUBE]):
            transit_daily[TRN_BART_TUBE][i] += val
        for i, val in enumerate(v[TRN_BART_SM]):
            transit_daily[TRN_BART_SM][i] += val
        for i, val in enumerate(v[TRN_CALTRAIN_SM]):
            transit_daily[TRN_CALTRAIN_SM][i] += val
        for i, val in enumerate(v[TRN_GGT]):
            transit_daily[TRN_GGT][i] += val
        for i, val in enumerate(v[TRN_SAMTRANS]):
            transit_daily[TRN_SAMTRANS][i] += val
        for i, val in enumerate(v[TRN_ACTRANSIT]):
            transit_daily[TRN_ACTRANSIT][i] += val 
        for i, val in enumerate(v[TRN_XFER_LINKED]):
            transit_daily[TRN_XFER_LINKED][i] += val
    return transit_daily



# Check if the SFALLMSA_* CSV files exist
csv_files = [AM_csv ,PM_csv ,MD_csv ,EV_csv ,EA_csv ]
missing_csv_files = []
for csv_file in csv_files:
    if not os.path.exists(csv_file):
        missing_csv_files.append(csv_file)
if not missing_csv_files:
    print("CSV files with prefix SFALLMSA exist")
else:
    # Check if the corresponding DBF files exist
    dbf_files = [AM_dbf,PM_dbf,MD_dbf,EV_dbf,EA_dbf]
    missing_dbf_files = []
    for dbf_file in dbf_files:
        if not os.path.exists(dbf_file):
            missing_dbf_files.append(dbf_file)
    if missing_dbf_files:
        print(f"DBF files with prefix SFALLMSA missing: {missing_dbf_files}")
    else:
        for dbf_file in dbf_files:
            csv_file = os.path.join(WORKING_FOLDER, os.path.splitext(os.path.basename(dbf_file))[0] + ".csv")
            dbf = Dbf5(dbf_file)

            dbf.to_csv(csv_file)

        print("DBF files with prefix SFALLMSA converted to CSV files")


AM = pd.read_csv(AM_csv, low_memory=False)
PM = pd.read_csv(PM_csv, low_memory=False)
EV = pd.read_csv(EV_csv, low_memory=False)
EA = pd.read_csv(EA_csv, low_memory=False)
MD = pd.read_csv(MD_csv, low_memory=False)

transit_am = getTransitBoardings(AM,"AM")
transit_pm = getTransitBoardings(PM,"PM")
transit_ev = getTransitBoardings(EV,"EV")
transit_ea = getTransitBoardings(EA,"EA")
transit_md = getTransitBoardings(MD,"MD")

res_dict = {"AM":transit_am, "EA":transit_ea,"EV":transit_ev,"MD":transit_md,"PM":transit_pm}

output = {'Daily':get_total_transit(res_dict), "AM":transit_am, "PM": transit_pm}
for timeperiod in ['Daily', "AM", "PM"]:
    muni_boarding_types = [TRN_MUNI_GEARY, TRN_MUNI_VN, TRN_MUNI_MISSION, 
                   TRN_MUNI_RAIL,TRN_MUNI_BUS, TRN_MUNI_TOTAL,TRN_XFER_LINKED ]
    muni_rows = [['Muni Boardings', 'San Francisco']]
    for i in muni_boarding_types:
        muni_rows.append([i]+[int(output[timeperiod][i][0])])
    rate = muni_rows[-2][1]/muni_rows[-1][1]
    muni_rows.append([TRN_XFER_RATE, round(rate,2)])
    rail_vol_rows = [['Rail Volumes', 'Inbound', 'Outbound']]
    rail_vol_types  = [TRN_BART_TUBE, TRN_BART_SM, TRN_CALTRAIN_SM ]
    for i in rail_vol_types:
        row = [int(round(j)) for j in output[timeperiod][i]]
        rail_vol_rows.append([i]+row)
    rail_board_types  = [ TRN_BART, TRN_CALTRAIN ]
    rail_boarding_rows = [['Rail Boardings', 'Downtown', 'San Francisco', 'Bay Area']]
    for i in rail_board_types:
        row = [int(round(j)) for j in output[timeperiod][i]]
        rail_boarding_rows.append([i]+row)
    bus_vol_types     = [  TRN_ACTRANSIT, TRN_GGT,TRN_SAMTRANS ]
    bus_vol_rows = [['Bus Volumes', 'Inbound', 'Outbound']]
    for i in bus_vol_types:
        row = [int(round(j)) for j in output[timeperiod][i]]
        bus_vol_rows.append([i]+row)
    df_muni_ridership = pd.DataFrame(muni_rows[1:], columns=muni_rows[0]).set_index(muni_rows[0][0])
    df_rail_boardings = pd.DataFrame(rail_boarding_rows[1:], columns=rail_boarding_rows[0]).set_index(rail_boarding_rows[0][0])
    df_rail_volume = pd.DataFrame(rail_vol_rows[1:], columns=rail_vol_rows[0]).set_index(rail_vol_rows[0][0])
    df_bus_volume = pd.DataFrame(bus_vol_rows[1:], columns=bus_vol_rows[0]).set_index(bus_vol_rows[0][0])


    output_folder = OUTPUT_FOLDER 
    output_file_name = OUTPUT_FILENAME
    for i, df in enumerate([df_muni_ridership, df_rail_boardings, df_rail_volume, df_bus_volume]):
        index_name = df.index.name.lower().replace(' ', '_')
        filename = f'{output_file_name}_{index_name}_{timeperiod}.md'
        markdown_table = tabulate(df, headers='keys', tablefmt='pipe', floatfmt='.1f').split('\n')
        markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
        markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
        markdown_table = '\n'.join(markdown_table)
        with open(f'{output_folder}/{filename}', 'w') as f:
            f.write(markdown_table)


    # Write the DataFrames to a combined CSV file with a blank line between each DataFrame
        with open(f'{output_folder}/{output_file_name}_{timeperiod}.csv', 'w') as f:
            df_muni_ridership.to_csv(f, index=True, header=True)
            f.write('\n')
            df_rail_boardings.to_csv(f, index=True, header=True)
            f.write('\n')
            df_rail_volume.to_csv(f, index=True, header=True)
            f.write('\n')
            df_bus_volume.to_csv(f, index=True, header=True)
    # Write each DataFrame to a separate markdown file

    selected_muni_rows = df_muni_ridership.loc[df_muni_ridership.index.isin(['Geary Corridor(38 & 38L)', 'Van Ness (47 & 49)', 'Mission (49 & 14)'])]
    # write selected rows to CSV file
    selected_muni_rows.to_csv(f'{output_folder}/{output_file_name}_sel_muni_{timeperiod}.csv', index=True, header=[ 'boardings'])
    df_rail_boardings_slice = df_rail_boardings.reset_index()
    df_melted_rb = pd.melt(df_rail_boardings_slice, id_vars=['Rail Boardings'], var_name='direction', value_name='value')
    df_melted_rb = df_melted_rb.rename(columns={'Rail Boardings': 'line'})
    df_melted_rb = df_melted_rb[['line', 'direction', 'value']]
    df_melted_rb.to_csv(f'{output_folder}/{output_file_name}_sel_rb_{timeperiod}.csv', index=False)
    df_rail_volume_slice = df_rail_volume.reset_index()
    df_melted_rv = pd.melt(df_rail_volume_slice, id_vars=['Rail Volumes'], var_name='direction', value_name='value')
    df_melted_rv = df_melted_rv.rename(columns={'Rail Volumes': 'line'})
    df_melted_rv = df_melted_rv[['line', 'direction', 'value']]
    df_melted_rv.to_csv(f'{output_folder}/{output_file_name}_sel_rv_{timeperiod}.csv', index=False)
    df_bus_volume_slice = df_bus_volume.reset_index()
    df_melted_bv = pd.melt(df_bus_volume_slice, id_vars=['Bus Volumes'], var_name='direction', value_name='value')
    df_melted_bv = df_melted_bv.rename(columns={'Bus Volumes': 'line'})
    df_melted_bv = df_melted_bv[['line', 'direction', 'value']]
    df_melted_bv.to_csv(f'{output_folder}/{output_file_name}_sel_bv_{timeperiod}.csv', index=False)



