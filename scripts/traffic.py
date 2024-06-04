import pandas as pd
from socket import gethostname
import configparser
from simpledbf import Dbf5 as dbf
import os, sys, subprocess
from utilTools import DataFrameToCustomHTML
from pathlib import Path

# Extract folder settings from the control file
CTL_FILE = r"topsheet.ctl"
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER = os.getenv('WORKING_FOLDER')
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER')
SCRIPT_FOLDER = Path(config["folder_setting"]["SCRIPT_FOLDER"])


# Extract input file names from the control file
HWY_CREATEDAILY_CMD = os.path.join(
    SCRIPT_FOLDER, config["traffic"]["Traffic_LOADEXPORT_CMD"]
)
HWY_VOLFILES = {
    "AM": config["traffic"]["am_vols_dbf"],
    "MD": config["traffic"]["md_vols_dbf"],
    "PM": config["traffic"]["pm_vols_dbf"],
    "EV": config["traffic"]["ev_vols_dbf"],
    "EA": config["traffic"]["ea_vols_dbf"],
    "Daily": config["traffic"]["daily_vols_dbf"],
}

#define traffic constants

HWY_ALLCOUNTYLINES = "All SF County Lines"
HWY_ROWS = [
    HWY_ALLCOUNTYLINES,
    "SM County Line",
    "Bridges",
    ["Bay Bridge", 1],
    ["GG Bridge", 1],
    ["San Rafael Bridge (WB,EB)", 1],
    ["San Mateo Bridge (WB,EB)", 1],
    ["Dumbarton Bridge (WB,EB)", 1],
    ["Carquinez Bridge", 1],  # ['Benicia-Martinez Bridge',1],
    "Octavia @ Market (NB,SB)",
    "19th Ave @ Lincoln (NB,SB)",
    "Geary/O'Farrell @ VN (EB,WB)",
    "Van Ness @ Geary (NB,SB)",
]
HWY_SCREENS = {
    "Bay Bridge": [
        [52832, 52494],  # inbound
        [52495, 52833],  # outbound
    ],
    "GG Bridge": [
        [52267, 52425],  # inbound
        [52426, 52268],  # outbound
    ],
    "San Rafael Bridge (WB,EB)": [
        [103342, 8894],  # inbound = westbound
        [8853, 103341],  # outbound = eastbound
    ],
    "San Mateo Bridge (WB,EB)": [
        [7381, 7393],  # inbound = westbound
        [7382, 7383],  # outbound = eastbound
    ],
    "Dumbarton Bridge (WB,EB)": [
        [6938, 6921],  # inbound = westbound
        [6922, 6939],  # outbound = eastbound
    ],
    "Carquinez Bridge": [
        [12643, 12667, 12643, 12634],  # inbound
        [12637, 12642, 10482, 10483],  # outbound
    ],
    "Benicia-Martinez Bridge": [
        [3131, 3136, 3131, 10247],  # inbound
        [3135, 3130, 3132, 3142],  # outbound
    ],
    "SM County Line": [  # done by looking @ network and loading bayarea_county.shhp
        # just documenting all cross points
        # the big 3 (101, 280, 35) all match the roadway validation spreadsheet
        [
            7678,
            51171,  # inbound - 35
            7677,
            33539,  # lake merced
            51450,
            33467,  # junipero serra
            51130,
            22527,  # saint charles
            52270,
            22515,  # de long
            52234,
            52271,  # 280 north
            51137,
            22513,  # santa cruz
            52456,
            51138,  # santa barbara
            33704,
            33454,  # shakespeare st
            33704,
            22482,  # rhine st
            51142,
            22482,  # flournoy st
            51113,
            22479,  # wilson st
            51113,
            22464,  # san jose ave
            52461,
            22464,  # goethe st
            52462,
            22465,  # rice st
            52463,
            22452,  # liebig st
            52787,
            21584,  # mission st
            52792,
            33702,  # irvington st
            52469,
            33702,  # acton st
            52459,
            21531,  # brunswick st
            52790,
            21531,  # oliver st
            51019,
            21500,  # whittier st
            51019,
            21493,  # hanover st
            52774,
            21493,  # lowell st
            33603,
            21461,  # guttenberg st
            33844,
            33351,  # pope st
            51000,
            33351,  # polaris way
            52487,
            51009,  # cordova st
            52487,
            21371,  # canyon way
            51002,
            21371,  # south hill blvd
            52489,
            21359,  # carter st
            52482,
            20421,  # geneva ave
            52482,
            20413,  # pasadena st
            52483,
            20412,  # castillo st
            52481,
            20411,  # pueblo str
            52484,
            20402,  # calgary st
            52485,
            20403,  # rio verde st
            33605,
            20373,  # velasco ave
            33605,
            20375,  # schwerin st
            50995,
            20306,  # bayshore
            6986,
            50856,  # tunnel ave
            6985,
            52492,  # alana way
            40029,
            52118,  # hwy 101nb
            40003,
            50852,
        ],  # harney way
        [
            51171,
            7678,  # outbound - 35
            33549,
            7677,  # lake merced
            22528,
            51449,  # alemany blvd
            52480,
            51449,  # junipero serra
            22527,
            51130,  # saint charles
            52137,
            52236,  # 280 s offramp
            52137,
            52235,  # another 280 s offramp
            22515,
            52270,  # de long
            22513,
            51137,  # santa cruz
            51138,
            52456,  # santa barbara
            33454,
            33704,  # shakespeare st
            22482,
            33704,  # rhine st
            22482,
            51142,  # flournoy st
            22479,
            51113,  # wilson st
            22464,
            51113,  # san jose ave
            22464,
            52461,  # goethe st
            22465,
            52462,  # rice st
            22452,
            52463,  # liebig st
            21584,
            52787,  # mission st
            33702,
            52792,  # irvington st
            33702,
            52469,  # acton st
            21531,
            52459,  # brunswick st
            21531,
            52790,  # oliver st
            21500,
            51019,  # whittier st
            21493,
            51019,  # hanover st
            21493,
            52774,  # lowell st
            21461,
            33603,  # guttenberg st
            33351,
            33844,  # pope st
            33351,
            51000,  # polaris way
            51009,
            52487,  # cordova st
            21371,
            52487,  # canyon way
            21371,
            51002,  # south hill blvd
            21359,
            52489,  # carter st
            20421,
            52482,  # geneva ave
            20413,
            52482,  # pasadena st
            20412,
            52483,  # castillo st
            20411,
            52481,  # pueblo str
            20402,
            52484,  # calgary st
            20403,
            52485,  # rio verde st
            20373,
            33605,  # velasco ave
            20375,
            33605,  # schwerin st
            20306,
            50995,  # bayshore
            50856,
            6986,  # tunnel ave
            52492,
            6985,  # alana way
            52264,
            7732,  # hwy 101sb
            69013,
            10689,  # hwy 101sb HOV
            50852,
            40003,
        ],  # harney way
    ],
    "Octavia @ Market (NB,SB)": [
        [30756, 25858, 30756, 53029],  # inbound
        [53029, 30756, 53028, 30756],  # oubound
    ],
    "Geary/O'Farrell @ VN (EB,WB)": [
        [25196, 25192],  # inbound
        [25197, 25213],  # outbound
    ],
    "Van Ness @ Geary (NB,SB)": [
        [25195, 25197],  # nb
        [25197, 30709],  # sb
    ],
    "19th Ave @ Lincoln (NB,SB)": [
        [27372, 27374],  # nb
        [27374, 27372],  # sb
    ],
}


def runtpp(scriptname, outputfile, env=None):
    """Runs the given tpp script if the given outputfile doesn't exist"""
    if not os.stat(outputfile):
        # it doesn't exist, run it
        hostname = gethostname()
        fullenv = None
        if env:
            fullenv = os.environ
            fullenv.update(env)
        # dispatch it, no cube license
        if hostname == "Mason" or hostname == "D90V07K1-okforgetit":
            f = open("topsheet.tmp", "w")
            f.write("runtpp " + scriptname + "\n")
            f.close()
            scriptname = scriptname.replace("/", "\\")
            dproc = subprocess.Popen(
                "Y:/champ/util/bin/dispatch.bat topsheet.tmp ocean", env=fullenv
            )
            dret = dproc.wait()
        else:
            dproc = subprocess.Popen("runtpp " + scriptname, env=fullenv)
            dret = dproc.wait()
        print("dret = ", dret)
    return


def getTrafficScreens(timePeriod="Daily"):
    """
    Extracts and aggregates traffic volume data for predefined screenlines based on the time period.

    Parameters:
    - timePeriod (str): The time period for which the traffic data is required. Defaults to "Daily".

    Returns:
    - dict: A dictionary with keys as screenline names and values as lists [inbound traffic volume, outbound traffic volume].

    This function performs the following tasks:
    1. Determines the file path for highway volume data based on the time period.
    2. If necessary, creates daily volume data using a predefined command.
    3. Reads the volume data from a DBF file into a DataFrame.
    4. Sets up a mapping from segment identifiers to screenline names and direction (inbound or outbound).
    5. Loops through the DataFrame to aggregate traffic volumes for each screenline.
    6. Converts volume totals to integers for cleaner output.
    7. Aggregates volumes for specific county lines into a combined traffic count.

    The screenline mapping and volume data are predefined. The function relies on global variables and external commands
    to manipulate and access the data.
    """
    hwyfile = os.path.join(WORKING_FOLDER, HWY_VOLFILES[timePeriod])
    runtpp(HWY_CREATEDAILY_CMD, hwyfile)  # create it, if needed
    file_dbf = dbf(hwyfile)
    hwydbf = file_dbf.to_dataframe()
    abToScreen = {}
    traffic = {}
    if timePeriod == "Daily":
        vol = "DAILY_TOT"
    else:
        vol = "TOTVOL"
    # build a reverse mapping for what we want
    ## We can use numpy or direct for key,value in dict.itmes() instead of so many loops
    for key, value in HWY_SCREENS.items():
        # inbound
        for index in range(0, len(value[0]), 2):
            abToScreen[str(value[0][index]) + " " + str(value[0][index + 1])] = [key, 0]
        # outbound
        for index in range(0, len(value[1]), 2):
            abToScreen[str(value[1][index]) + " " + str(value[1][index + 1])] = [key, 1]
    for i in range(len(hwydbf)):
        rec = hwydbf.loc[i]
        if rec["AB"] in abToScreen:
            screen, inout = abToScreen[rec["AB"]][0], abToScreen[rec["AB"]][1]
            if screen not in traffic:
                traffic[screen] = [0, 0]  # 1st inbound, 2nd outbound
            traffic[screen][inout] = traffic[screen][inout] + rec[vol]
    # make them ints
    for screen in traffic:
        traffic[screen][0] = int(traffic[screen][0])
        traffic[screen][1] = int(traffic[screen][1])

    # aggregate
    traffic[HWY_ALLCOUNTYLINES] = [0, 0]
    traffic[HWY_ALLCOUNTYLINES][0] = (
        traffic["SM County Line"][0]
        + traffic["Bay Bridge"][0]
        + traffic["GG Bridge"][0]
    )
    traffic[HWY_ALLCOUNTYLINES][1] = (
        traffic["SM County Line"][1]
        + traffic["Bay Bridge"][1]
        + traffic["GG Bridge"][1]
    )
    return traffic

# 1.traffic_dow.md
time = ["AM", "MD", "PM", "EV", "EA"]
dow_df = pd.DataFrame()
for t in time:
    dict_t = getTrafficScreens(t)
    df_t = pd.DataFrame(data=dict_t)
    tra_pm = df_t.transpose().reset_index()
    index_order = [11, 1, 10, 9, 3, 2, 0, 4, 8, 7, 6, 5]
    tra_pm = tra_pm.reindex(index_order, axis=0)
    tra_pm.columns = ["Lines", f"{t} In", f"{t} Out"]
    # print(tra_pm)
    if t == "AM":
        dow_df = tra_pm.copy()
    else:
        dow_df = pd.merge(dow_df, tra_pm)
dow_df.index = dow_df["Lines"]
dow_df.drop("Lines", axis=1, inplace=True)
even_columns = dow_df.iloc[:, ::2]
even_sum = even_columns.sum(axis=1)
odd_columns = dow_df.iloc[:, 1::2]
odd_sum = odd_columns.sum(axis=1)
dow_df["Total Indbound"] = even_sum
dow_df["Total Outbound"] = odd_sum
dow_df.reset_index(inplace=True)
md_path = os.path.join(OUTPUT_FOLDER, "traffic_dow.md")
df2html = DataFrameToCustomHTML([], [0])
df2html.generate_html(dow_df, md_path)


def trafficMaps(timePeriod="Daily"):
    """
    Generates a CSV file of traffic volumes for different vehicle categories based on the specified time period.

    Parameters:
    - timePeriod (str): The time period for which to generate the traffic map, defaults to "Daily".

    The function performs the following tasks:
    1. Constructs the path to the highway volume data file based on the time period.
    2. Reads the volume data from the file into a DataFrame.
    3. Depending on the time period, selects the relevant columns for daily or specific time-period data.
    4. Writes the selected traffic data to a CSV file in the designated output folder.
    
    This function is utilized for generating detailed traffic volume maps which can be used for further analysis or reporting.
    """

    hwyfile = os.path.join(WORKING_FOLDER, HWY_VOLFILES[timePeriod])
    file_dbf = dbf(hwyfile)
    hwydbf = file_dbf.to_dataframe()
    if timePeriod != "Daily":
        data = hwydbf[
            ["AB", "DA", "SR2", "SR3", "COM", "TRK", "BUS", "TNC", "AUTOVOL", "TOTVOL"]
        ]
    else:
        data = hwydbf[
            [
                "AB",
                "DAILY_DA",
                "DAILY_SR2",
                "DAILY_SR3",
                "DAILY_COM",
                "DAILY_TRK",
                "DAILY_BUS",
                "DAILY_TNC",
                "DAILY_AUTO",
                "DAILY_TOT",
            ]
        ]
    csv_path = os.path.join(OUTPUT_FOLDER, "traffic_map_" + timePeriod + ".csv")
    data.to_csv(
        csv_path,
        index=False,
        header=[
            "AB",
            "DA",
            "SR2",
            "SR3",
            "COM",
            "TRK",
            "BUS",
            "TNC",
            "AUTOVOL",
            "TOTVOL",
        ],
    )

# 2. traffic_map_Daily.csv
trafficMaps()


def getTypeTrafficScreens(timePeriod="Daily"):
    """
    Aggregates and returns traffic volume data by vehicle type for each screenline based on the specified time period.

    Parameters:
    - timePeriod (str): Specifies the time period for which traffic data should be retrieved. Defaults to "Daily".

    Returns:
    - list: A list of dictionaries. Each dictionary corresponds to a specific vehicle type and contains traffic volume data
            for each screenline, both inbound and outbound.

    The function performs the following steps:
    1. Constructs the path to the highway volume data file based on the time period and loads it into a DataFrame.
    2. Establishes a mapping from segment identifiers to screenline names and direction (inbound or outbound).
    3. Iterates over each vehicle type, accumulating traffic volumes for each screenline segment.
    4. Converts traffic volume totals to integers for clearer analysis.
    5. Aggregates total traffic volumes for county lines and returns a structured list of traffic data.

    This method is useful for traffic analysis where detailed breakdown by vehicle type is needed for specific screenlines over different time periods.
    """
    hwyfile = os.path.join(WORKING_FOLDER, HWY_VOLFILES[timePeriod])
    file_dbf = dbf(hwyfile)
    hwydbf = file_dbf.to_dataframe()
    abToScreen = {}
    v_class = []
    volume = [
        "DAILY_DA",
        "DAILY_SR2",
        "DAILY_SR3",
        "DAILY_TRK",
        "DAILY_COM",
        "DAILY_BUS",
        "DAILY_TNC",
        "DAILY_AUTO",
    ]
    # build a reverse mapping for what we want
    ## We can use numpy or direct for key,value in dict.itmes() instead of so many loops
    for key, value in HWY_SCREENS.items():
        # inbound
        for index in range(0, len(value[0]), 2):
            abToScreen[str(value[0][index]) + " " + str(value[0][index + 1])] = [key, 0]
        # outbound
        for index in range(0, len(value[1]), 2):
            abToScreen[str(value[1][index]) + " " + str(value[1][index + 1])] = [key, 1]
    for vol in volume:
        traffic = {}
        for i in range(len(hwydbf)):
            rec = hwydbf.loc[i]
            if rec["AB"] in abToScreen:
                screen, inout = abToScreen[rec["AB"]][0], abToScreen[rec["AB"]][1]
                if screen not in traffic:
                    traffic[screen] = [0, 0]  # 1st inbound, 2nd outbound
                # if (screen == 'SM County Line'):
                #   print screen, inout, rec['AB'], int(rec[vol])
                traffic[screen][inout] = traffic[screen][inout] + rec[vol]
        # make them ints
        for screen in traffic:
            traffic[screen][0] = int(traffic[screen][0])
            traffic[screen][1] = int(traffic[screen][1])

        # aggregate
        traffic[HWY_ALLCOUNTYLINES] = [0, 0]
        traffic[HWY_ALLCOUNTYLINES][0] = (
            traffic["SM County Line"][0]
            + traffic["Bay Bridge"][0]
            + traffic["GG Bridge"][0]
        )
        traffic[HWY_ALLCOUNTYLINES][1] = (
            traffic["SM County Line"][1]
            + traffic["Bay Bridge"][1]
            + traffic["GG Bridge"][1]
        )
        v_class.append(traffic)
    return v_class


def lineFiles(dicts, line):
    """
    Transforms a list of dictionaries into a structured DataFrame showing inbound and outbound traffic volumes for different vehicle classes.

    Parameters:
    - dicts (list of dict): A list of dictionaries where each dictionary represents traffic data from different files.
    - line (str): The screenline identifier for which traffic data is needed.

    Returns:
    - DataFrame: A DataFrame containing structured traffic data for the specified screenline, 
                  including inbound and outbound volumes for various vehicle classes, and a total sum row.

    This function processes multiple traffic data entries, consolidates them into a single DataFrame,
    and calculates sum totals for a clear and concise overview of traffic volumes.
    """
    df1 = [pd.DataFrame(d) for d in dicts]
    df2 = pd.concat(df1, ignore_index=True).T
    mtables = df2.iloc[[1, 9, 10, 11]]
    inbound = mtables.loc[line, ::2].T.reset_index(drop=True)
    outbound = mtables.loc[line, 1::2].T.reset_index(drop=True)
    df = pd.concat([inbound, outbound], axis=1)
    df.index = ["DA", "SR2", "SR3", "TRK", "COM", "BUS", "TNC", "AUTO"]
    df.columns = ["Inbound", "Outbound"]
    sum_row = df.iloc[0:7].sum()
    # Convert the sum to a DataFrame and transpose it to make it a row
    sum_row_df = pd.DataFrame([sum_row])

    df = pd.concat([df, sum_row_df], ignore_index=True)
    df.index = ["DA", "SR2", "SR3", "TRK", "COM", "BUS", "TNC", "AUTO", "TOTAL"]
    df.reset_index(drop=False, inplace=True)
    df.columns = ["Class", "Inbound", "Outbound"]
    md_path = os.path.join(OUTPUT_FOLDER, "traffic_" + line + ".md")
    df2html = DataFrameToCustomHTML([], [0])
    df2html.generate_html(df, md_path)

#3 traffice_SM County Line.md, 4.traffice_GG Bridge.md, 5.traffice_Bay Bridge.md, 6.traffice_All SF County Lines.md,
dicts = getTypeTrafficScreens()
majorLines = ["SM County Line", "GG Bridge", "Bay Bridge", "All SF County Lines"]
for line in majorLines:
    lineFiles(dicts, line)
