import pandas as pd
from simpledbf import Dbf5
import re,os
from tabulate import tabulate
import configparser
import itertools
from utilTools import readEqvFile, DataFrameToCustomHTML, modifyDistrictNameForMap

# Set working and output directories from environment variables
WORKING_FOLDER          =  'input/landuse'
OUTPUT_FOLDER           =  'output'

# Retrieve the control file path from environment and read its contents
# This script requires the following files from the control file: 
# "tazdata.dbf", "modesumSimple3_RPM9.ctl", and "DIST15.eqv"

CTL_FILE                = 'topsheet.ctl'
config = configparser.ConfigParser()
config.read(CTL_FILE)

# Extract file names from the control file for land use
LU_FILE_NAME            =  config['land_use']['LU_FILE']
LU_CLEAN_FILE_NAME      =  config['land_use']['LU_CLEAN_FILE']
LU_CTLFILE_NAME         =  config['land_use']['LU_CTL_FILE']
DIST_EQV_NAME           =  config['land_use']['DIST_EQV']
AREA_EQV_NAME           =  config['land_use']['AREA_EQV']
OUTPUT_FILE_NAME        =  config['land_use']['LandUse_output_file_name']

# Construct absolute paths for the land use files
LU_FILE                 =  os.path.join(WORKING_FOLDER, LU_FILE_NAME)
LU_CLEAN_FILE           =  os.path.join(WORKING_FOLDER, LU_CLEAN_FILE_NAME)
LU_CTLFILE              =  os.path.join(WORKING_FOLDER, LU_CTLFILE_NAME)
DIST_EQV                =  os.path.join(WORKING_FOLDER, DIST_EQV_NAME)
AREA_EQV                =  os.path.join(WORKING_FOLDER, AREA_EQV_NAME)
# Define constants for land use categories
LU_HHLDS                = 'Households' 
LU_POP                  = 'Population' 
LU_EMPRES               = 'Employed Residents' 
LU_TOTALEMP             = 'Total Employment' 
LU_ROWS                 = [ LU_HHLDS, LU_POP, LU_EMPRES, LU_TOTALEMP ]

def getLanduseAttributesForDists(num_dists, taz_to_dist,LU_FILE):
    """
    Retrieve land use attributes for districts from the land use file.

    Args:
    - num_dists (int): Number of districts.
    - taz_to_dist (dict): Maps TAZ numbers to a list of district numbers.

    Returns:
    - landuse (dict): Maps land use categories to a list of values per district.
    """
    
    # Initialize the landuse dictionary with zeros
    landuse = {
        LU_HHLDS: [0] * num_dists, 
        LU_POP: [0] * num_dists,
        LU_EMPRES: [0] * num_dists, 
        LU_TOTALEMP: [0] * num_dists
    }

    # Convert the land use DBF to a pandas dataframe
    dbf = Dbf5(LU_FILE)
    df = dbf.to_dataframe()

    for _, row in df.iterrows():
        taz = row["SFTAZ"]
        
        # Warn if the TAZ is not part of any district
        if taz not in taz_to_dist:
            print(f"Taz {taz} not part of any district")
            continue

        # Update the land use attributes for all districts the TAZ is part of
        for dist in taz_to_dist[taz]:
            idx = dist - 1  # Adjust for zero-based indexing
            landuse[LU_HHLDS][idx] += row['HHLDS']
            landuse[LU_POP][idx] += row['POP']
            landuse[LU_EMPRES][idx] += row['EMPRES']
            landuse[LU_TOTALEMP][idx] += row['TOTALEMP']

    return landuse

def convertToCumulative(input_dict):
    """
    Convert a dictionary of lists to its cumulative counterpart.
    
    Args:
    - input_dict (dict): Dictionary where each key maps to a list of values.
    
    Returns:
    - Dictionary where each key maps to a cumulative sum list of the original values.
    """
    
    cum_dict = {}
    for attrib, values in input_dict.items():
        # Use Python's itertools.accumulate for a cumulative sum
        cum_dict[attrib] = list(itertools.accumulate(values))
    
    return cum_dict

def calcualte_densities(eqvfile,LU_FILE,SQ_FILE,accumulated):
    
    distnames, distToTaz, tazToDist, numdists = readEqvFile(eqvfile)
    tazdbf = Dbf5(SQ_FILE)
    tazdf = tazdbf.to_dataframe()
    tazdf['DISTRICT_NAME'] = tazdf.apply(lambda row : distnames[tazToDist[row['TAZ']][0]], axis=1)
    tazdf = tazdf.groupby('DISTRICT_NAME').sum('SQ_MILE').reset_index()
    if accumulated:
        res = convertToCumulative(getLanduseAttributesForDists(numdists, tazToDist,LU_FILE))
    else:
        res = getLanduseAttributesForDists(numdists, tazToDist,LU_FILE)
        
    df = pd.DataFrame.from_dict({var: list(map(int, res[var])) for var in res},
                            orient='index', columns=list(distnames.values()))
    df = df.T[['Households','Population','Employed Residents','Total Employment']].reset_index()
    
    mergedf = df.merge(tazdf[['DISTRICT_NAME', 'SQ_MILE']], left_on='index', right_on='DISTRICT_NAME', how='left')
    mergedf.drop(columns='DISTRICT_NAME', inplace=True)
    mergedf = mergedf.rename(columns={'index': 'DISTRICT_NAME'})
    mergedf['HH DENSITY'] = mergedf['Households'] / mergedf['SQ_MILE']
    mergedf['POP DENSITY'] = mergedf['Population'] / mergedf['SQ_MILE']
    mergedf['EMP RES DENSITY'] = mergedf['Employed Residents'] / mergedf['SQ_MILE']
    mergedf['TOTAL EMP DENSITY'] = mergedf['Total Employment'] / mergedf['SQ_MILE']
    mergedf = mergedf.rename(columns={'Households':'HH','Population':'POP','Employed Residents':'EMPRES','Total Employment':'JOB'})
    
    return mergedf
def merge_luFiles(LU_FILE, LU_CLEAN_FILE,taz=True):
    
    df = Dbf5(LU_FILE).to_dataframe()
    tazdf = Dbf5(LU_CLEAN_FILE).to_dataframe()
    filtered_df = df[df['SFTAZ'].isin(tazToDist.keys())]
    relevant_columns = ['SFTAZ', 'HHLDS', 'POP', 'EMPRES', 'CIE', 'MED', 'MIPS', 'PDR', 'RETAIL', 'VISITOR', 'TOTALEMP']
    merged_df = filtered_df[relevant_columns].merge(
        tazdf[['TAZ', 'SQ_MILE']], left_on='SFTAZ', right_on='TAZ', how='left'
    )
    merged_df['DISTRICT'] = merged_df['SFTAZ'].apply(lambda x: tazToDist[x][0])
    if not taz:
        merged_df = merged_df.groupby('DISTRICT').sum()
        merged_df['DISTRICT_NAME'] = merged_df.index.map(distnames)
    else:
        merged_df['DISTRICT_NAME'] = merged_df['DISTRICT'].map(distnames)
    merged_df['HH DENSITY'] = merged_df['HHLDS'] / merged_df['SQ_MILE']
    merged_df['POP DENSITY'] = merged_df['POP'] / merged_df['SQ_MILE']
    merged_df['EMP RES DENSITY'] = merged_df['EMPRES'] / merged_df['SQ_MILE']
    merged_df['TOTAL EMP DENSITY'] = merged_df['TOTALEMP'] / merged_df['SQ_MILE']
    
    return merged_df

if __name__ == "__main__":

    district_density_df = calcualte_densities(DIST_EQV ,LU_FILE,LU_CLEAN_FILE,False)
    area_density_df = calcualte_densities(AREA_EQV ,LU_FILE,LU_CLEAN_FILE,True)
    area_density_df.drop(0,inplace=True)
    area_density_df.loc[1,'DISTRICT_NAME'] = 'San Francisco'
    area_density_df.loc[2,'DISTRICT_NAME'] = 'Bay Area'
    area_density_df.iloc[[0, 1]] = area_density_df.iloc[[1, 0]].values

    combined_df = pd.concat([area_density_df, district_density_df], ignore_index=True)
    combined_df[['HH DENSITY','POP DENSITY','EMP RES DENSITY','TOTAL EMP DENSITY']] = combined_df[['HH DENSITY','POP DENSITY','EMP RES DENSITY','TOTAL EMP DENSITY']].astype(int)
    combined_df.drop('SQ_MILE',axis=1,inplace=True)
    combined_df = combined_df.set_index('DISTRICT_NAME')
    combined_df.index.name = 'Geography'
    combined_df = combined_df.reset_index()
    df2html = DataFrameToCustomHTML( [0,1,2,14,15,16],[0], [0])
    md_path = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}.md')

    df2html.generate_html(combined_df, md_path)

    csv_df = district_density_df[['DISTRICT_NAME','HH DENSITY',
        'POP DENSITY', 'EMP RES DENSITY', 'TOTAL EMP DENSITY']]

    csv_df = csv_df.set_index('DISTRICT_NAME')
    csv_df.index.name = 'Area'
    csv_df = csv_df.reset_index().melt(id_vars=['Area'], var_name='category', value_name='value')
    csv_path = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}.csv')
    csv_df.to_csv(csv_path, index=False)



    distnames, distToTaz, tazToDist, numdists = readEqvFile(DIST_EQV)
    merged_df = merge_luFiles(LU_FILE, LU_CLEAN_FILE)
    merged_df.rename(columns={
    'HH DENSITY': 'Households Density', 'POP DENSITY': 'Population Density', 'EMP RES DENSITY': 'Employed Residents Density',
    'EMPRES': 'Employed Residents Value', 'TOTALEMP': 'Employment Value', 'TOTAL EMP DENSITY': 'Employment Density',
    'HHLDS': 'Households Value', 'POP': 'Population Value'
}, inplace=True)
    taz_map_csv  = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}_density.csv')
    merged_df.to_csv(taz_map_csv,index=False)



    
    merged_df = merge_luFiles(LU_FILE, LU_CLEAN_FILE, False)

    columns = [ 'HHLDS', 'POP', 'EMPRES', 'TOTALEMP', 'HH DENSITY', 'POP DENSITY', 'EMP RES DENSITY', 'TOTAL EMP DENSITY', 'DISTRICT_NAME']
    final_df = merged_df[columns]
    final_df.rename(columns={
        'HH DENSITY': 'Households Density', 'POP DENSITY': 'Population Density', 'EMP RES DENSITY': 'Employed Residents Density',
        'EMPRES': 'Employed Residents Value', 'TOTALEMP': 'Employment Value', 'TOTAL EMP DENSITY': 'Employment Density',
        'HHLDS': 'Households Value', 'POP': 'Population Value'
    }, inplace=True)
    final_df.index.name = 'Area'
    area_adjustments = {
        2: 'N.Beach/', 3: 'Western', 4: 'Mission/', 5: 'Noe/', 6: 'Marina/', 9: 'Outer', 10: 'Hill'
    }
    final_df = final_df.reset_index()
    final_df = modifyDistrictNameForMap(final_df, 'DISTRICT_NAME')

    csv_path = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}_district.csv')
    final_df.to_csv(csv_path, index=False)
