import h5py
import pandas as pd
import sys, os
import subprocess
import configparser
from utilTools import (
    readEqvFile,
    DataFrameToCustomHTML,
    modifyDistrictNameForMap,
)
from pathlib import Path

sys.path.insert(0, os.path.join(os.environ['CHAMPVERSION'],'lib'))
from Lookups import Lookups, MAX_SF_COUNTY_ZONE

# Extract folder settings from the control file
CTL_FILE = r"topsheet.ctl"
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER = os.getenv('WORKING_FOLDER')
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER')

# Extract input file names from the control file
DIST_EQV = os.path.join(WORKING_FOLDER, config["mode_share"]["DIST_EQV"])
AM_h5_file = os.path.join(WORKING_FOLDER, config["mode_share"]["AM_h5_file"])
MD_h5_file = os.path.join(WORKING_FOLDER, config["mode_share"]["MD_h5_file"])
PM_h5_file = os.path.join(WORKING_FOLDER, config["mode_share"]["PM_h5_file"])
EV_h5_file = os.path.join(WORKING_FOLDER, config["mode_share"]["EV_h5_file"])
EA_h5_file = os.path.join(WORKING_FOLDER, config["mode_share"]["EA_h5_file"])
AM_mat_file = os.path.join(WORKING_FOLDER, config["mode_share"]["AM_mat_file"])
MD_mat_file = os.path.join(WORKING_FOLDER, config["mode_share"]["MD_mat_file"])
PM_mat_file = os.path.join(WORKING_FOLDER, config["mode_share"]["PM_mat_file"])
EV_mat_file = os.path.join(WORKING_FOLDER, config["mode_share"]["EV_mat_file"])
EA_mat_file = os.path.join(WORKING_FOLDER, config["mode_share"]["EA_mat_file"])
Convert_mat2h5 = os.path.join(WORKING_FOLDER, config["mode_share"]["Convert_mat2h5"])
rp_trips = os.path.join(
    WORKING_FOLDER, "daysim", "abm_output1", config["mode_share"]["RP_DISAG_TRIPS"]
)

TIMEPERIODS = ['EA','AM','MD','PM','EV']
MODES = ['Auto','Transit','Walk','Bike','TNC']

def convert_mat_to_h5(directory, mat_file):
    # Construct the full paths to the input and output files
    input_file = f"{os.path.join(directory, mat_file)}.mat"
    executable = os.path.join(directory, "mat2h5.exe")
    # Construct the command to run the conversion
    command = [executable, input_file]

    # Execute the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode == 0:
        print(f"Successfully converted {mat_file}.mat to {mat_file}.h5")
    else:
        print(f"Failed to convert {mat_file}.mat: {result.stderr}")

def convert_persontrips():
    # Convert MAT files to h5 files if missing
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
            mat_file_names = [
                "PERSONTRIPS_AM",
                "PERSONTRIPS_EA",
                "PERSONTRIPS_EV",
                "PERSONTRIPS_MD",
                "PERSONTRIPS_PM",
            ]
            for mat_file in mat_file_names:
                convert_mat_to_h5(WORKING_FOLDER, mat_file)    
                
def mode_share_from_trip_list(df, o_label, d_label, idx_label, scale=1.0):
    '''
    o_lable: taz or district origin field
    d_lable: taz or district destination field
    idx_label: name for geography; taz or district
    '''
    df = (final_df.pivot_table(index=o_label, columns='mode', values='trexpfac', aggfunc='sum') + 
                final_df.pivot_table(index=d_label, columns='mode', values='trexpfac', aggfunc='sum'))
    df.index.name=idx_label
    df[3] = df[3] + df[4] + df[5] # collapse auto modes
    df[6] = df[6] + df[8] # collapse transit and school bus
    df = (df.drop(columns=[4, 5, 8])
                        .rename(columns={1: 'Walk', 2: 'Bike', 3: 'Auto', 6:'Transit', 9: 'TNC'})[['Auto','Transit','Walk','Bike','TNC']])
    # constructed with transposes due to pandas bug dividing along rows
    df = df.T.divide(df.T.sum()).T * scale
    return df
            
if __name__=='__main__':


    # TRIP TABLE SUMMARIES
    convert_persontrips()
    
    
    distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)
    taz_to_dist = {taz: dist[0] for taz, dist in tazToDist.items()}
    
    tp_to_name = {
        "AM": AM_h5_file,
        "MD": MD_h5_file,
        "PM": PM_h5_file,
        "EV": EV_h5_file,
        "EA": EA_h5_file,
    }
    tazmats = {tp: {} for tp in TIMEPERIODS}
    distmats = {tp: {} for tp in TIMEPERIODS}
    matrix_mode_num_to_name = {1: 'Auto', 2: 'Auto', 3: 'Auto', # auto, auto toll, auto paid
                               4: 'Walk', 5: 'Bike', 6: 'Transit', 7: 'Transit', # walk, bike, walk-transit, drive-transit
                               20: 'TNC'}

    # store all tables in the "tables" dictionary
    for tp, filename in tp_to_name.items():
        with h5py.File(filename, "r") as f:
            for num, name in matrix_mode_num_to_name.items():
                tazmat = pd.DataFrame(f[str(num)][...])
                tazmat.index = tazmat.index + 1
                tazmat.columns = tazmat.columns + 1
                
                # get just the tazs that belong to districts  TODO: CHECK THIS HANDLES EXTERNALS
                distmat = tazmat.loc[taz_to_dist.keys(),taz_to_dist.keys()]
                distmat.rename(index={x: taz_to_dist[x] for x in distmat.columns},
                               columns={x: taz_to_dist[x] for x in distmat.columns},
                               inplace=True)
                distmat = distmat.groupby(distmat.index).sum().T.groupby(distmat.index).sum().T
                
                if not name in tazmats[tp]:
                    tazmats[tp][name] = tazmat  # get all the dataset
                else:
                    tazmats[tp][name] += tazmat  # get all the dataset

                if not name in distmats[tp]:
                    distmats[tp][name] = distmat  # get all the dataset
                else:
                    distmats[tp][name] += distmat  # get all the dataset

    sf_mode_by_tp = pd.DataFrame(data=0.0, index=pd.Index(TIMEPERIODS, name='tp'), columns=MODES)
    mode_by_dist = pd.DataFrame(data=0.0, index=pd.Index(distToTaz.keys(), name='District No'), columns=MODES)
    mode_by_taz = pd.DataFrame(data=0.0, index=pd.Index(tazToDist.keys(), name='TAZ'), columns=MODES)
    
    for tp in TIMEPERIODS:
        for mode in MODES:
            tazmat = tazmats[tp][mode]
            distmat = distmats[tp][mode]
            
            # for each of these, row sum + column sum - intersection.
            
            # sf modesum by tp
            sf_mode_by_tp.loc[tp, mode] = (tazmat.loc[:MAX_SF_COUNTY_ZONE,:].sum().sum() +
                                                 tazmat.loc[:,:MAX_SF_COUNTY_ZONE].sum().sum() -
                                                 tazmat.loc[:MAX_SF_COUNTY_ZONE,:MAX_SF_COUNTY_ZONE].sum().sum()
                                                )
                                                
            # taz daily modesum
            mode_by_taz[mode] += tazmat.sum() + tazmat.sum(axis=1) - [tazmat.loc[i, i] for i in tazmat.index]
            
            # regional district daily modesum
            for dist in distToTaz.keys():
                mode_by_dist.loc[dist, mode] = ((distmat.loc[:dist,:].sum().sum() +
                                                       distmat.loc[:,:dist].sum().sum() -
                                                       distmat.loc[:dist,:dist].sum().sum()
                                                      ))
                
    sf_mode_by_tp.loc['Daily'] = sf_mode_by_tp.sum()
    sf_mode_by_tp = (sf_mode_by_tp.T.divide(sf_mode_by_tp.T.sum()).T * 100.0).round(1)
    sf_mode_by_tp = sf_mode_by_tp.reset_index()
    df2html = DataFrameToCustomHTML([], [0])
    df2html.generate_html(sf_mode_by_tp, os.path.join(OUTPUT_FOLDER, "sf_mode_by_tp_src_trip_tables.md"), True)
    sf_mode_by_tp.to_csv(os.path.join(OUTPUT_FOLDER, "sf_mode_by_tp_src_trip_tables.csv"), index=False)

    mode_by_dist = (mode_by_dist.T.divide(mode_by_dist.T.sum()).T * 100.0).round(1)
    mode_by_dist.insert(0, 'District', mode_by_dist.index.map(lambda x: distnames[x]))
    mode_by_dist = mode_by_dist.reset_index()
    
    df2html = DataFrameToCustomHTML([], [0, 1])
    df2html.generate_html(mode_by_dist, os.path.join(OUTPUT_FOLDER, "dist15_mode_src_trip_tables.md"), True)
    mode_by_dist.to_csv(os.path.join(OUTPUT_FOLDER, "dist15_mode_src_trip_tables.csv"), index=False)

    mode_by_taz = (mode_by_taz.T.divide(mode_by_taz.T.sum()).T * 100.0).round(1)
    mode_by_taz.to_csv(os.path.join(OUTPUT_FOLDER, "taz_mode_src_trip_tables.csv"))


    # TRIP LIST SUMMARIES
    necessary_columns = ["dpurp","tour_id","tseg","deptm","endacttm","otaz","mode","dtaz","trexpfac"]
    df = pd.read_csv(rp_trips, delimiter="\t", usecols=necessary_columns)

    df["original_index"] = df.index
    df_temp = df[df["dpurp"] == 10].copy()
    df_temp["tseg"] += 1
    merged_df = pd.merge(
        df,
        df_temp,
        left_on=["tour_id", "tseg", "deptm"],
        right_on=["tour_id", "tseg", "endacttm"],
        suffixes=("", "_next"),
    )
    merged_df["deptm"] = merged_df["deptm_next"]
    merged_df["otaz"] = merged_df["otaz_next"]
    merged_df["mode"] = 6

    indices_to_delete = []
    indices_to_delete.extend(list(merged_df["original_index"]))
    indices_to_delete.extend(list(merged_df["original_index_next"]))
    cols_to_drop = [col for col in merged_df.columns if col.endswith("_next")]
    merged_df.drop(cols_to_drop, axis=1, inplace=True)
    df_cleaned = df.drop(indices_to_delete)
    
    final_df = pd.concat([merged_df, df_cleaned], ignore_index=True)
    final_df['tp'] = 'EV'
    final_df.loc[final_df['deptm'].between(180,360),'tp'] = 'EA'
    final_df.loc[final_df['deptm'].between(360,540),'tp'] = 'AM'
    final_df.loc[final_df['deptm'].between(540,930),'tp'] = 'MD'
    final_df.loc[final_df['deptm'].between(930,1110),'tp'] = 'PM'

    final_df["odist"] = final_df["otaz"].map(lambda x: tazToDist[x][0])
    final_df["ddist"] = final_df["dtaz"].map(lambda x: tazToDist[x][0])

    # 7.dist15_mode_src_trip_list.md, 8.dist15_mode_src_trip_list.csv, 9. district_sf_mode_by_tp_src_trip_list_map.csv
    df = mode_share_from_trip_list(final_df,'odist','ddist','District No',scale=100.0).round(1)
    df.insert(0, 'District',df.index.map(lambda x: distnames[x]))

    df = df.reset_index()
    df2html = DataFrameToCustomHTML([], [0, 1])
    df2html.generate_html(df, os.path.join(OUTPUT_FOLDER, "dist15_mode_src_trip_list.md"), True)
    df.to_csv(os.path.join(OUTPUT_FOLDER, "dist15_mode_src_trip_list.csv"), index=False)

    # 10. sf_mode_by_tp_src_trip_list.md, 11.sf_mode_by_tp_src_trip_list.csv"
    sf = {}
    df = (final_df.loc[final_df['otaz'].le(981) | final_df['dtaz'].le(981)]
                  .pivot_table(index='tp', columns='mode', values='trexpfac', aggfunc='sum'))      
    df[3] = df[3] + df[4] + df[5] # collapse auto modes
    df[6] = df[6] + df[8] # collapse transit and school bus
    df = (df.drop(columns=[4, 5, 8])
            .rename(columns={1: 'Walk', 2: 'Bike', 3: 'Auto', 6:'Transit', 9: 'TNC'}))
    df = df.loc[['EA','AM','MD','PM','EV'],['Auto','Transit','Walk','Bike','TNC']]
    df.loc['Daily',:] = df.sum()
    df = (df.T.divide(df.T.sum()).T * 100.0).round(1)
    df = df.reset_index()

    df2html = DataFrameToCustomHTML([], [0])
    df2html.generate_html(df, os.path.join(OUTPUT_FOLDER, "sf_mode_by_tp_src_trip_list.md"), True)
    df.to_csv(os.path.join(OUTPUT_FOLDER, "sf_mode_by_tp_src_trip_list.csv"), index=False)

    # 12. taz_mode_tab.csv
    taz_mode = mode_share_from_trip_list(final_df,'otaz','dtaz','TAZ',scale=100.0).round(1)
    taz_mode.to_csv(os.path.join(OUTPUT_FOLDER, "taz_mode_src_trip_list.csv"))

