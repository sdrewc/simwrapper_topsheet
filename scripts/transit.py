import datetime,os,re,sys,subprocess
import pandas as pd
from simpledbf import Dbf5
import configparser
from platform import node
import pandas as pd
from shapely.geometry import Point, LineString
import geopandas as gpd
import sys, os
import numpy as np
from utilTools import DataFrameToCustomHTML
from pathlib import Path



CTL_FILE                = r'../topsheet.ctl'
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER            =  Path(config['folder_setting']['WORKING_FOLDER'])
OUTPUT_FOLDER           =  Path(config['folder_setting']['OUTPUT_FOLDER'])



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
FREEFRLOWNODES_dbf      =  os.path.join(WORKING_FOLDER, config['transit']['FREEFLOW_nodes_DBF'])

linked_muni_files = {
    'AM': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_AM_DBF']),
    'PM': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_PM_DBF']),
    'MD': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_MD_DBF']),
    'EV': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_EV_DBF']),
    'EA': os.path.join(WORKING_FOLDER, config['transit']['LINKEDMUNI_EA_DBF'])
}
OUTPUT_FILENAME         =  config['transit']['Transit_File_Name']
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
            dbf = Dbf5(FREEFRLOWNODES_dbf)
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
    if len(tmp) == 0:
        return [0, 0]
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
def is_integer(value):
    return isinstance(value, (int, float)) and value.is_integer()


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
timeperiod = 'Daily'
muni_boarding_types = [TRN_MUNI_GEARY, TRN_MUNI_VN, TRN_MUNI_MISSION, 
                TRN_MUNI_RAIL,TRN_MUNI_BUS, TRN_MUNI_TOTAL,TRN_XFER_LINKED]
muni_rows = [['Muni Boardings', 'San Francisco']]
for i in muni_boarding_types:
    muni_rows.append([i]+[int(output[timeperiod][i][0])])
rate = muni_rows[-2][1]/muni_rows[-1][1]
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
df_muni_ridership['San Francisco'] = df_muni_ridership['San Francisco'].apply(lambda x: f"{x:,}")
df_muni_ridership.loc[TRN_XFER_RATE] = f'{round(rate,2)}'
df_rail_boardings = pd.DataFrame(rail_boarding_rows[1:], columns=rail_boarding_rows[0]).set_index(rail_boarding_rows[0][0])
df_rail_volume = pd.DataFrame(rail_vol_rows[1:], columns=rail_vol_rows[0]).set_index(rail_vol_rows[0][0])
df_bus_volume = pd.DataFrame(bus_vol_rows[1:], columns=bus_vol_rows[0]).set_index(bus_vol_rows[0][0])
for df in [df_muni_ridership, df_rail_boardings, df_rail_volume, df_bus_volume]:
    index_name = df.index.name.lower().replace(' ', '_')
    df = df.reset_index()
    filename = f'Transit_{index_name}_{timeperiod}.md'
    md_path = os.path.join(OUTPUT_FOLDER, filename)
    df2html = DataFrameToCustomHTML( [],[0])
    df2html.generate_html(df, md_path)

class SimwrapperMapConstructor():
    def __init__(self, link_data_path, node_data_path, bus_output, rail_output, reg_output):
        self.link_data_path = link_data_path
        self.node_data_path = node_data_path
        self.bus_output = bus_output
        self.rail_output = rail_output
        self.reg_output = reg_output
        self.caltrain_nodes = [14688, 14687, 14686, 14685, 14684, 14683, 14682, 14681, 14680,
         14679, 14678, 14677, 14676, 14675, 14673, 14672, 14671, 14670,
         14669, 14668, 14667, 14665, 14664, 14663, 14662, 14661, 14660,
         14659, 14658, 14656, 14655]
    
    def build_maps(self,):
        node_xy = pd.read_csv(self.node_data_path, header=None)
        node_xy['xy'] = pd.Series([Point(x, y) for x, y in zip(node_xy[1], node_xy[2])])
        node_xy.rename(columns={0: 'A', 'xy': 'A_xy'}, inplace=True)
        df = self.fix_caltrain_shapes()
        df = df.merge(node_xy[['A', 'A_xy']])
        node_xy.rename(columns={'A': 'B', 'A_xy': 'B_xy'}, inplace=True)
        df = df.merge(node_xy[['B', 'B_xy']])
        df['geometry'] = pd.Series([LineString([x, y]) for x, y in zip(df['A_xy'], df['B_xy'])])
        df.drop(['A_xy', 'B_xy'], axis=1, inplace=True)
        geo_df = gpd.GeoDataFrame(df[['A','B','MODE', 'geometry']])
        geo_df['AB'] = geo_df['A'].astype(str) + ' ' + geo_df['B'].astype(str)
        geo_df = geo_df.drop_duplicates(subset=['AB','MODE'])
        geo_df.crs = "EPSG:2227"
        desired_modes = [11,12,13]
        bus = geo_df[geo_df['MODE'].isin(desired_modes)]
        bus = bus[['AB','geometry']]
        bus.to_file(self.bus_output)
        desired_modes = [15]
        rail = geo_df[geo_df['MODE'].isin(desired_modes)]
        rail = rail[['AB','geometry']]
        rail.to_file(self.rail_output)
        desired_modes = [22,23,24,26,31,32]
        reg = geo_df[geo_df['MODE'].isin(desired_modes)]
        reg = reg[['AB','geometry']]
        reg.to_file(self.reg_output)
    def fix_caltrain_shapes(self):
        df = pd.read_csv(self.link_data_path,low_memory=False)
        ct = df.loc[df['MODE'].eq(26) & df['A'].isin(self.caltrain_nodes) & df['B'].isin(self.caltrain_nodes)]
        new_rows_all = []
        for idx, row in ct.iterrows():
            new_rows = self.split_link(row, self.caltrain_nodes)
            new_rows.index = pd.RangeIndex(start=idx*10, stop=idx*10+len(new_rows), step=1) / 10
            new_rows_all.append(new_rows)
        df_res = df.loc[~df.index.isin(ct.index)]
        df = pd.concat([df_res]+new_rows_all).sort_index()
        return df
    
    def split_link(self, row, nodes):
        data = []
        iA, iB = nodes.index(row['A']), nodes.index(row['B'])
        
        if abs(iA-iB) == 1:
            return pd.DataFrame(data=[[row['A'], row['B'], row['MODE'], row['NAME'], row['AB_VOL']
                        ]], columns=['A','B','MODE','NAME','AB_VOL'])
        
        if iA > iB:
            iA = len(nodes) - iA - 1
            iB = len(nodes) - iB - 1
            nodes = nodes[::-1]
        
        for n1, n2 in zip(nodes[iA:iB], nodes[iA+1:iB+1]):
            data.append([n1, n2, row['MODE'], row['NAME'], row['AB_VOL']
                        ])
        df = pd.DataFrame(data=data, columns=['A','B','MODE','NAME','AB_VOL'])
        return df
    
class MapDataConstructor():
    def __init__(self,am_datapath, pm_datapath, am_output, pm_output):
        self.am_file = am_datapath
        self.pm_file = pm_datapath
        self.am_df = pd.read_csv(self.am_file, low_memory=False)
        self.pm_df = pd.read_csv(self.pm_file, low_memory=False)
        self.am_output = am_output
        self.pm_output = pm_output
        self.caltrain_nodes = [14688, 14687, 14686, 14685, 14684, 14683, 14682, 14681, 14680,
             14679, 14678, 14677, 14676, 14675, 14673, 14672, 14671, 14670,
             14669, 14668, 14667, 14665, 14664, 14663, 14662, 14661, 14660,
             14659, 14658, 14656, 14655]
        self.peakHour_factor = {'AM': 0.348, 'MD': 0.154, 'PM':0.337, 'EV': 0.173, 'EA': 0.463}
        self.tp_duration = {'AM':3, 'MD': 6.5,'PM':3, 'EV':8.5, 'EA': 3}
        self.desired_modes = [11,12,13,15,22,23,24,26,31,32]

    def create_data(self):
        self.am_df = self.fix_caltrain_data(self.am_df)
        self.pm_df = self.fix_caltrain_data(self.pm_df)
        self.am_df = self.get_crowd(self.am_df, "AM")
        self.pm_df = self.get_crowd(self.pm_df, "PM")
        self.am_df = self.am_df[self.am_df['MODE'].isin(self.desired_modes)].copy()
        self.pm_df = self.pm_df[self.pm_df['MODE'].isin(self.desired_modes)].copy()
        def concatenate_columns(row):
            return str(row['A']) + ' ' + str(row['B'])
        self.am_df.loc[:, 'AB'] = self.am_df.apply(concatenate_columns, axis = 1)
        self.pm_df.loc[:, 'AB'] = self.pm_df.apply(concatenate_columns, axis = 1)
        self.am_df = self.am_df.groupby('AB', as_index=False).agg({'AB_VOL': 'sum','crowd':'max'})
        self.pm_df = self.pm_df.groupby('AB', as_index=False).agg({'AB_VOL': 'sum','crowd':'max'})
        self.am_df.to_csv(self.am_output,index=False)
        self.pm_df.to_csv(self.pm_output,index=False)


    def get_crowd(self,df, tp):
        agg_df = df.groupby('AB').agg(AB_VOL_sum=('AB_VOL', 'sum'),
                                PERIODCAP_sum=('PERIODCAP', 'sum')).reset_index()
        agg_df['crowd'] = (agg_df['AB_VOL_sum'] *  self.peakHour_factor[tp]) / (agg_df['PERIODCAP_sum'] / self.tp_duration[tp])

        df = df.merge(agg_df[['AB', 'crowd']], on='AB', how='left')
        
        return df
    
    def fix_caltrain_data(self,df):
        ct = df.loc[df['MODE'].eq(26) & df['A'].isin(self.caltrain_nodes) & df['B'].isin(self.caltrain_nodes)]

        new_rows_all = []

        for idx, row in ct.iterrows():
            new_rows = self.split_link(row, self.caltrain_nodes)
            # order this index after the removed row
            new_rows.index = pd.RangeIndex(start=idx*10, stop=idx*10+len(new_rows), step=1) / 10
            new_rows_all.append(new_rows)

        df_res = df.loc[~df.index.isin(ct.index)]

        df = pd.concat([df_res]+new_rows_all).sort_index()
        for col in ['AB_VOL']:
            df[col] = df[col].map(lambda x: round(x,3))
        df = df.reset_index().sort_values(by=['A','B'])
        return df


    def split_link(self, row, nodes):
        data = []
        iA, iB = nodes.index(row['A']), nodes.index(row['B'])

        if abs(iA-iB) == 1:
            return pd.DataFrame(data=pd.DataFrame(data=[[row['A'], row['B'], row['MODE'], row['AB_VOL']]], columns=['A','B','MODE','AB_VOL']))

        if iA > iB:
            iA = len(nodes) - iA - 1
            iB = len(nodes) - iB - 1
            nodes = nodes[::-1]

        for n1, n2 in zip(nodes[iA:iB], nodes[iA+1:iB+1]):
            data.append([n1, n2, row['MODE'], row['AB_VOL']])
        df = pd.DataFrame(data=data, columns=['A','B','MODE','AB_VOL'])
        return df

link_data_path = AM_csv
node_data_path = os.path.join(WORKING_FOLDER,'cubenet_validate_nodes.csv')
bus_output = os.path.join(OUTPUT_FOLDER,'bus.shp')
rail_output = os.path.join(OUTPUT_FOLDER,'rail.shp')
reg_output = os.path.join(OUTPUT_FOLDER,'reg.shp')

mapConstructor = SimwrapperMapConstructor(link_data_path, node_data_path, bus_output, rail_output, reg_output)
mapConstructor.build_maps()

am_datapath = AM_csv
pm_datapath = PM_csv
am_output = os.path.join(OUTPUT_FOLDER,"reg_am.csv")
pm_output = os.path.join(OUTPUT_FOLDER,"reg_pm.csv")
dataConstructor = MapDataConstructor(am_datapath, pm_datapath, am_output, pm_output)
dataConstructor.create_data()