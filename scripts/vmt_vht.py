import pandas as pd
import os, subprocess
import configparser
from utilTools import DataFrameToCustomHTML
from pathlib import Path


# Read parameters from control file
CTL_FILE = r"../topsheet.ctl"
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER = Path(config["folder_setting"]["WORKING_FOLDER"])
OUTPUT_FOLDER = Path(config["folder_setting"]["OUTPUT_FOLDER"])
SCRIPT_FOLDER = Path(config["folder_setting"]["SCRIPT_FOLDER"])
# Extract input file names from the control file
NET_FILES = [
    config["vmt_vht"]["AM_vmt_vht_file"],
    config["vmt_vht"]["PM_vmt_vht_file"],
    config["vmt_vht"]["MD_vmt_vht_file"],
    config["vmt_vht"]["EV_vmt_vht_file"],
    config["vmt_vht"]["EA_vmt_vht_file"],
]
script_path = os.path.join(SCRIPT_FOLDER, config["vmt_vht"]["NET2CSV"])


# Function to run the Cube Voyager script for a given NET file
def run_cube_script(directory, script_path, net_file):
    # Environment variable setup
    env = os.environ.copy()
    env["CUBENET"] = os.path.join(directory, net_file)

    command = f'runtpp "{script_path}"'

    # Execute the command
    result = subprocess.run(
        command, shell=True, env=env, capture_output=True, text=True
    )

    # Check if the command was successful
    if result.returncode == 0:
        print(f"Successfully processed {net_file}")
    else:
        print(f"Failed to process {net_file}: {result.stderr}")


# Process each file if missing
for file in NET_FILES:
    if not os.path.exists(os.path.join(WORKING_FOLDER, file + ".csv")):
        run_cube_script(WORKING_FOLDER, script_path, file)

VMT_VMT = "VMT"
VMT_VHT = "VHT"
VMT_VMTOVERVHT = "VMT/VHT"
VMT_LOSTTIME = "Lost VH (vs freeflow)"
VMT_ROWS = [VMT_VMT, VMT_VHT, VMT_VMTOVERVHT, VMT_LOSTTIME]


def getVmtRaw(timePeriod):
    """
    Calculates vehicle miles traveled (VMT) and related metrics for different segments and conditions based on the specified time period.

    Parameters:
    - timePeriod (str): The time period for which VMT data is being calculated. This can be 'AM', 'MD', 'PM', 'EV', 'EA', or 'Daily'.

    Returns:
    - list: A list containing VMT and vehicle hours traveled (VHT) metrics for San Francisco (SF) and the region, including lost time due to congestion.
    """
    if timePeriod == "Daily":
        am_arr = getVmtRaw("AM")
        md_arr = getVmtRaw("MD")
        pm_arr = getVmtRaw("PM")
        ev_arr = getVmtRaw("EV")
        ea_arr = getVmtRaw("EA")
        daily_arr = []
        for ind in range(0, len(am_arr)):
            daily_arr.append(
                am_arr[ind] + md_arr[ind] + pm_arr[ind] + ev_arr[ind] + ea_arr[ind]
            )
        return daily_arr

    time_factors = {"AM": 0.44, "MD": 0.18, "PM": 0.37, "EV": 0.22, "EA": 0.58}
    file = os.path.join(WORKING_FOLDER, "LOAD" + timePeriod + "_FINAL.csv")
    df = pd.read_csv(file, on_bad_lines="skip")
    df = df.loc[df["FT"] != 6]
    peakHourFactor = time_factors.get(timePeriod)
    df.loc[df["SPEED"] == 0, "time_freeflow"] = 0
    df.loc[df["SPEED"] != 0, "time_freeflow"] = (df["DISTANCE"] / df["SPEED"]) * 60
    df["lost_time"] = ((df["TIME_1"] - df["time_freeflow"]) / 60) * df["V_1"]
    vmt_sf = df.loc[df["MTYPE"] == "SF", ["DISTANCE", "V_1"]].prod(axis=1).sum()
    vht_sf = df.loc[df["MTYPE"] == "SF", ["TIME_1", "V_1"]].prod(axis=1).sum() / 60
    losttime_sf = df.loc[df["MTYPE"] == "SF", "lost_time"].sum()
    vmt_region = df[["DISTANCE", "V_1"]].prod(axis=1).sum()
    vht_region = df[["TIME_1", "V_1"]].prod(axis=1).sum() / 60
    losttime_region = df["lost_time"].sum()
    vmt_region_losf = (
        df.loc[
            df["V_1"] * peakHourFactor / (df["CAP"] * df["LANE_AM"] + 0.00001) > 1,
            ["DISTANCE", "V_1"],
        ]
        .prod(axis=1)
        .sum()
    )
    filtered_df = df.loc[df["MTYPE"] == "SF"]
    selected_columns = ["DISTANCE", "V_1"]
    filtered_columns = filtered_df[selected_columns]
    vmt_sf_losf = (
        filtered_df.loc[
            filtered_df["V_1"]
            * peakHourFactor
            / (filtered_df["CAP"] * filtered_df["LANE_AM"] + 0.00001)
            > 1,
            ["DISTANCE", "V_1"],
        ]
        .prod(axis=1)
        .sum()
    )
    vmt_nonsf = vmt_region - vmt_sf
    vht_nonsf = vht_region - vht_sf
    losttime_nonsf = losttime_region - losttime_sf
    vmt_nonsf_losf = vmt_region_losf - vmt_sf_losf
    return [vmt_sf, vht_sf, losttime_sf, vmt_region, vht_region, losttime_region]


def getVmt(timePeriod="Daily"):
    """
    Calculates vehicle miles traveled (VMT), vehicle hours traveled (VHT), average speeds, and lost vehicle hours for San Francisco and the rest of the Bay Area.

    Parameters:
    - timePeriod (str): The time period for which metrics are calculated. Defaults to "Daily".

    Returns:
    - dict: A dictionary containing VMT, VHT, speed, and lost vehicle hours.
    """
    raw = getVmtRaw(timePeriod)
    vmt = {}
    vmt["VMT"] = (raw[0], raw[3] - raw[0], raw[3])
    vmt["VHT"] = (raw[1], raw[4] - raw[1], raw[4])
    vmt["Speed"] = (
        raw[0] / raw[1],
        (raw[3] - raw[0]) / (raw[4] - raw[1]),
        raw[3] / raw[4],
    )
    vmt["Lost VH (vs freeflow)"] = (raw[2], raw[5] - raw[2], raw[5])
    return vmt


def format_with_commas(x):
    return "{:,.0f}".format(x)


def dictToMD(res, res_index, index_name, index_list, output_file):
    """
    Converts a dictionary containing transit data into a Markdown file for reporting.

    Parameters:
    - res (dict): The dictionary containing transit data.
    - res_index (int): The index to select data from nested items.
    - index_name (str): Name of the index column.
    - index_list (list): A list of indices, such as time periods.
    - output_file (str): The name of the output Markdown file.

    Processes:
    - Formats data into a DataFrame.
    - Converts DataFrame to HTML.
    - Saves HTML output to a Markdown file.
    """
    formatted_data = {"VMT": [], "VHT": [], "Speed": [], "Lost VH (vs freeflow)": []}
    for tp in index_list:
        for key, value in res[tp].items():
            if key == "Speed":
                formatted_data[key].append(round(value[res_index], 1))
            else:
                formatted_data[key].append(format_with_commas(round(value[0])))
    df = pd.DataFrame(data=formatted_data, index=index_list)
    df = df.T
    df.index.name = "TOD"
    df = df.reset_index()
    md_path = os.path.join(OUTPUT_FOLDER, f"{output_file}.md")
    df2html = DataFrameToCustomHTML([], [0])
    df2html.generate_html(df, md_path)


def getVC(region, output_file, index_name):
    """
    Calculates and saves vehicle categories and their contributions as percentages and raw values to CSV and Markdown files.

    Parameters:
    - region (str): Specifies the region of interest ('sf', 'nonsf', or 'ba').
    - output_file (str): The base name for the output files (CSV and Markdown).
    - index_name (str): Name of the index column (not explicitly used in the function).

    Processes:
    - Reads multiple data files.
    - Filters and aggregates data by vehicle category for specified regions.
    - Computes total and proportional statistics.
    - Outputs results in both CSV and Markdown format.
    """
    res = {
        "Drive Alone": [],
        "Shared Ride 2": [],
        "Shared Ride 3+": [],
        "Trucks": [],
        "Commercial Vehicles": [],
        "TNC": [],
    }
    for file in NET_FILES:
        file += ".csv"
        csv_file = os.path.join(WORKING_FOLDER, file)
        df = pd.read_csv(csv_file, on_bad_lines="skip")
        df = df.loc[df["FT"] != 6]
        if region == "sf":
            df = df.loc[df["MTYPE"] == "SF"]
        elif region == "nonsf":
            df = df.loc[df["MTYPE"] != "SF"]
        elif region == "ba":
            pass
        res["Drive Alone"].append(
            df[["DISTANCE", "V1_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V4_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V7_1"]].prod(axis=1).sum()
        )
        res["Shared Ride 2"].append(
            df[["DISTANCE", "V2_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V5_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V8_1"]].prod(axis=1).sum()
        )
        res["Shared Ride 3+"].append(
            df[["DISTANCE", "V3_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V6_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V9_1"]].prod(axis=1).sum()
        )
        res["Trucks"].append(
            df[["DISTANCE", "V10_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V11_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V12_1"]].prod(axis=1).sum()
        )
        res["Commercial Vehicles"].append(
            df[["DISTANCE", "V13_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V14_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V15_1"]].prod(axis=1).sum()
        )
        res["TNC"].append(
            df[["DISTANCE", "V16_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V17_1"]].prod(axis=1).sum()
            + df[["DISTANCE", "V18_1"]].prod(axis=1).sum()
        )
    df = pd.DataFrame(data=res, index=["AM", "MD", "PM", "EV", "EA"])
    df = df.T
    class_sums = df.sum()

    for column in df.columns:
        df[column] = round(df[column] / class_sums[column], 4)
    df_melt = df.reset_index().melt(
        id_vars="index", var_name="Time", value_name="Percentage"
    )
    df_melt.columns = ["Category", "Time", "Percentage"]
    csv_path = os.path.join(OUTPUT_FOLDER, f"{output_file}.csv")
    df_melt.to_csv(csv_path, index=False)
    for key, values in res.items():
        tmp = []
        for value in values:
            tmp.append(format_with_commas(round(value)))
        res[key] = tmp

    df = pd.DataFrame(data=res, index=["AM", "MD", "PM", "EV", "EA"])
    df = df.T
    df.index.name = "TOD"
    df = df.reset_index()
    md_path = os.path.join(OUTPUT_FOLDER, f"{output_file}.md")
    df2html = DataFrameToCustomHTML([], [0])
    df2html.generate_html(df, md_path)

# 1.vmt_Daily.md
res = {
    "Daily": getVmt(),
    "AM": getVmt("AM"),
    "PM": getVmt("PM"),
    "MD": getVmt("MD"),
    "EV": getVmt("EV"),
    "EA": getVmt("EA"),
}

timeperiod = "Daily"
formatted_data = {}
for key, value in res[timeperiod].items():
    if key == "Speed":
        formatted_data[key] = [str(round(v, 1)) for v in value]
    else:
        formatted_data[key] = tuple(format_with_commas(x) for x in value)
df = pd.DataFrame(data=formatted_data, index=["SF", "Rest of Bay Area", "Bay Area"])

df = df.T
df.index.name = "Geography"
df = df.reset_index()
df2html = DataFrameToCustomHTML([], [0])
md_path = os.path.join(OUTPUT_FOLDER, "vmt_Daily.md")
df2html.generate_html(df, md_path)
formatted_data = {"VMT": [], "VHT": [], "Speed": [], "Lost VH (vs freeflow)": []}
for tp in ["AM", "MD", "PM", "EV", "EA"]:
    for key, value in res[tp].items():
        if key == "Speed":
            formatted_data[key].append(round(value[0], 1))
        else:
            formatted_data[key].append(format_with_commas(round(value[0])))
df = pd.DataFrame(data=formatted_data, index=["AM", "MD", "PM", "EV", "EA"])
df = df.T
df.index.name = "TOD"
df = df.reset_index()
# 2.vmt_ba.md, 3.vmt_sf.md, 4.vmt_nonsf.md, 
dictToMD(res, 0, "TOD", ["AM", "MD", "PM", "EV", "EA"], "vmt_sf")
dictToMD(res, 1, "TOD", ["AM", "MD", "PM", "EV", "EA"], "vmt_nonsf")
dictToMD(res, 2, "TOD", ["AM", "MD", "PM", "EV", "EA"], "vmt_ba")
# 5.vmt_vc_ba.md, 6.vmt_vc_sf.md, 7.vmt_vc_nonsf.md, 8.vmt_vc_ba.csv, 9.vmt_vc_sf.csv, 10.vmt_vc_nonsf.csv
getVC("sf", "vmt_vc_sf", "TOD")
getVC("nonsf", "vmt_vc_nonsf", "TOD")
getVC("ba", "vmt_vc_ba", "TOD")
