import numpy as np
import pandas as pd
from socket import gethostname
import configparser
from simpledbf import Dbf5 as dbf
import datetime,os,re,sys,subprocess
from tables import open_file
from utilTools import readEqvFile, readCtlFile, modifyDistrictNameForMap,DataFrameToCustomHTML
from pathlib import Path



CTL_FILE                = r'../topsheet.ctl'
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  Path(config['folder_setting']['WORKING_FOLDER'])
OUTPUT_FOLDER           =  Path(config['folder_setting']['OUTPUT_FOLDER'])

rp_hh                   = os.path.join(WORKING_FOLDER, 'daysim', 'abm_output1', config['resident_purpose']['RP_HH'])
rp_person               = os.path.join(WORKING_FOLDER, 'daysim', 'abm_output1', config['resident_purpose']['RP_PERSON'])
rp_tour                 = os.path.join(WORKING_FOLDER, 'daysim', 'abm_output1', config['resident_purpose']['RP_TOUR'])
rp_trips                = os.path.join(WORKING_FOLDER, 'daysim', 'abm_output1', config['resident_purpose']['RP_DISAG_TRIPS'])
dist_eqv                = os.path.join(WORKING_FOLDER, config['resident_purpose']['DIST_EQV'])
area_eqv                = os.path.join(WORKING_FOLDER, config['resident_purpose']['AREA_EQV'])



class ResidentPurposes:
    RP_HH           = rp_hh
    RP_PERSON       = rp_person
    RP_TOUR         = rp_tour
    RP_DISAG_TRIPS  = rp_trips
    RP_ROWS         = [ 'Work', 'School','Escort','Personal Business & Medical','Shopping','Meals',
                       'Social & Recreational','Change Mode','Return Home','Total' ]
    RP_ROWS_TOUR    = [ 'Work', 'Grade School', 'High School', 'College', 'Other', 'Workbased','Escort',
                    'Personal Business(including medical)','Social & Recreational','Shopping','Meals','Total' ]
    RP_ROWS_SIMPLIFIED   = [ 'Work', 'School','Return Home' ,'Other']
    RP_PURPOSES     = { 1:RP_ROWS[0], 2:RP_ROWS[1], 3:RP_ROWS[2], 4:RP_ROWS[3], 5:RP_ROWS[4], 6:RP_ROWS[5] }
    
    def __init__(self, eqvfile):
        (self.distToName, self.distToTaz, self.tazToDist, _numdists ) = readEqvFile(eqvfile)
        self.purposesByTimeperiod = False
        self.trip_file = os.path.join(WORKING_FOLDER,'daysim', 'abm_output1', "trips_temp.h5")
        self.tour_file = os.path.join(WORKING_FOLDER,'daysim', 'abm_output1', "tours_temp.h5")
        tp_dict = { "Daily":None, "AM":None, "PM":None, "EV":None, "MD":None,"EA":None}
        self.purposes_tour = tp_dict.copy()
        self.purpose = { "district": {"otaz":tp_dict.copy(), "hhtaz": tp_dict.copy(), "dtaz": tp_dict.copy()}}
    
    def getSimplifiedChampPurpose(self, row):
        xfer_purp = row['dpurp']
        if xfer_purp == 1:
            return ResidentPurposes.RP_ROWS_SIMPLIFIED[0] # Work
        elif xfer_purp == 2:
            return ResidentPurposes.RP_ROWS_SIMPLIFIED[1] # School
        elif xfer_purp == 0:
            return ResidentPurposes.RP_ROWS_SIMPLIFIED[2] #Home
        else:
            return ResidentPurposes.RP_ROWS_SIMPLIFIED[3] # Other

    def getChampPurpose(self, row,  tour=False):
        # Determine which purpose mapping to use
        xfer_purp_key = 'pdpurp' if tour else 'dpurp'
        xfer_purp = row[xfer_purp_key]

        # Additional logic for tour related purposes
        if tour:
            ptype = row['pptyp']
            if xfer_purp == 1:
                return ResidentPurposes.RP_ROWS_TOUR[0] # Work
            if xfer_purp == 2:
                if ptype == 7:  # child age 5-15
                    return ResidentPurposes.RP_ROWS_TOUR[1]  # Grade School
                elif ptype == 6:  # child age 16+
                    return ResidentPurposes.RP_ROWS_TOUR[2]  # High School
                else:
                    return ResidentPurposes.RP_ROWS_TOUR[3]  # College
            elif xfer_purp == 3:
                return ResidentPurposes.RP_ROWS_TOUR[6] #Escorts
            elif xfer_purp == 4:
                return ResidentPurposes.RP_ROWS_TOUR[7] #Personal business
            elif xfer_purp == 5:
                return ResidentPurposes.RP_ROWS_TOUR[9] #Shopping
            elif xfer_purp == 6:
                return ResidentPurposes.RP_ROWS_TOUR[10] # Meals
            elif xfer_purp == 7:
                return ResidentPurposes.RP_ROWS_TOUR[8] #Social
            elif xfer_purp == 10:
                return None
            else:
                return ResidentPurposes.RP_ROWS_TOUR[4] # Other

        purpose_map = {
            0: ResidentPurposes.RP_ROWS[8],  # Home
            1: ResidentPurposes.RP_ROWS[0],  # Work
            2: ResidentPurposes.RP_ROWS[1],  # School
            3: ResidentPurposes.RP_ROWS[2],  # Escort
            4: ResidentPurposes.RP_ROWS[3],  # Personal Business
            5: ResidentPurposes.RP_ROWS[4],  # Shopping
            6: ResidentPurposes.RP_ROWS[5],  # Meals
            7: ResidentPurposes.RP_ROWS[6],  # Social
            10: ResidentPurposes.RP_ROWS[7]  # Change Mode
        }

        return purpose_map.get(xfer_purp, ResidentPurposes.RP_ROWS[4])  # Other as default

    def getTimePeriod(self, row,  tour=False):
        '''EA = early monring(3am till 6am), AM = monring a.m. peak(6am till 9am), 
        MD = midday(9am till 3.30pm), PM = p.m peak(3.30pm till 6.30pm) and EV = evening(6.30pm till 3am)
         ---- Time is given in minutes'''

        if tour:
            use_time = row['tlvorig']
        else: 
            seg_dir = row['half']
            dep_time = row['deptm']
            arr_time = row['arrtm']
            use_time = arr_time if seg_dir==1 else dep_time
        if use_time>=180 and use_time<=359:
            return 'EA'
        elif use_time>=360 and use_time<=539:
            return 'AM'
        elif use_time>=540 and use_time<=929:
            return 'MD'
        elif use_time>=930 and use_time<=1109:
            return 'PM'
        elif use_time>=1110 or use_time<=179:
            return 'EV'
        else:
            return None
        # otherwise get it
    def create_trip(self):

        trip_df = pd.read_csv(ResidentPurposes.RP_DISAG_TRIPS, sep='\t', 
                              usecols = ['hhno','pno','tour_id','dpurp','half','deptm','arrtm', 'otaz','dtaz'])
        tour_df = pd.read_csv(ResidentPurposes.RP_TOUR, sep='\t', usecols = ['id','parent'])
        hh_df   = pd.read_csv(ResidentPurposes.RP_HH, sep='\t', usecols = ['hhno','hhtaz'])
        per_df  = pd.read_csv(ResidentPurposes.RP_PERSON, sep='\t', usecols = ['hhno','pno','pptyp'])
        
        trip_df = trip_df.merge(hh_df, how='left')
        trip_df = trip_df.merge(per_df, how='left')
        trip_df = trip_df.merge(tour_df, how='left', left_on='tour_id', right_on='id')
        trip_df['TimePeriod'] = trip_df.apply(self.getTimePeriod, axis=1)
        
        trip_df['trip_purpose'] = trip_df.apply(self.getChampPurpose, axis=1)

        trip_df['origin_district'] = trip_df['otaz'].map(lambda x: self.tazToDist[x][0] if x in self.tazToDist else None)
        trip_df['destination_district'] = trip_df['dtaz'].map(lambda x: self.tazToDist[x][0] if x in self.tazToDist else None)
        trip_df['res_district'] = trip_df['hhtaz'].map(lambda x: self.tazToDist[x][0] if x in self.tazToDist else None)
        
        trip_df = trip_df[['TimePeriod','trip_purpose',\
                            'origin_district','destination_district','res_district','otaz','dtaz','hhtaz']]
        store = pd.HDFStore(self.trip_file, 'w')
        store.put('root', trip_df, format='t')
        store.close()

    def getResidentPurposes(self, mode, timePeriod,sourceTaz="hhtaz" ):
        """
        *rundir* is where we'll read the TRIPMC file
        *timePeriod* can be one of "Daily", "AM" or "PM"
        
        Returns dictionary of purpose (Work,School,etc) -> 
           list of trips for that purpose for each district
        """

                        # "taz":{"otaz":tp_dict, "hhtaz": tp_dict, "dtaz": tp_dict},
        if mode == 'district' and self.purpose[mode][sourceTaz][timePeriod] is not None:
            return self.purpose[mode][sourceTaz][timePeriod]
        if not os.path.exists(self.trip_file):
            self.create_trip()

        dist_map = {"hhtaz":'res_district', "otaz":'origin_district', 'dtaz':'destination_district'}
        columns_to_load = ['TimePeriod', 'trip_purpose', dist_map[sourceTaz] if mode == "district" else sourceTaz]
        pivot_df = None
        with pd.HDFStore(self.trip_file, 'r') as store:
            chunksize = 100000  # Adjust chunksize based on system's memory capacity
            iterator = store.select('root', columns=columns_to_load, chunksize=chunksize)

            for df in iterator:
                if timePeriod != 'Daily':
                    df = df[df['TimePeriod'] == timePeriod]

                grouped = df.groupby([dist_map[sourceTaz] if mode == "district" else sourceTaz, 'trip_purpose']).size().reset_index(name='counts')
                temp_pivot = grouped.pivot(index='trip_purpose', columns=dist_map[sourceTaz] if mode == "district" else sourceTaz, values='counts').T

                if pivot_df is None:
                    pivot_df = temp_pivot
                else:
                    pivot_df = pivot_df.add(temp_pivot, fill_value=0)
        if pivot_df is not None:
            pivot_df.columns.name = None
            pivot_df['District'] = pivot_df.index

        if mode == "district":
            self.purpose[mode][sourceTaz][timePeriod] = pivot_df
        return pivot_df



    def create_tour(self):

        tour_df = pd.read_csv(ResidentPurposes.RP_TOUR, sep='\t', usecols = ['id','hhno','pno','parent','pdpurp','tlvorig'])
        hh_df   = pd.read_csv(ResidentPurposes.RP_HH, sep='\t', usecols = ['hhno','hhtaz'])
        per_df  = pd.read_csv(ResidentPurposes.RP_PERSON, sep='\t', usecols = ['hhno','pno','pptyp'])

        tour_df = tour_df.merge(hh_df, how='left',on='hhno')
        tour_df = tour_df.merge(per_df, how='left',on=['pno','hhno'])
        tour_df['TimePeriod'] = tour_df.apply(lambda row: self.getTimePeriod(row, True),axis=1)
        tour_df['purpose'] = tour_df.apply(lambda row: self.getChampPurpose(row, True),axis=1)
        tour_df['district'] = tour_df['hhtaz'].map(lambda x: self.tazToDist[x][0] if x in self.tazToDist else None)    

        tour_df = tour_df[['TimePeriod','purpose',\
                            'district','hhtaz']]
        store = pd.HDFStore(self.tour_file, 'w')
        store.put('root', tour_df, format='t')
        store.close()

    def getResidentPurposesTour(self, timePeriod):
        if self.purposes_tour[timePeriod] is not None:
            return self.purposes_tour[timePeriod]
        if not os.path.exists(self.tour_file):
            self.create_tour()

        with pd.HDFStore(self.tour_file, 'r') as store:
            df = store.select('root')
        if timePeriod != 'Daily':
            df = df[df['TimePeriod'] == timePeriod]

        grouped = df.groupby(['district', 'purpose' ]).size().reset_index(name='counts')
        pivot_df = grouped.pivot(index='purpose', columns= 'district', values='counts').T
        pivot_df.columns.name = None
        pivot_df['District'] = pivot_df.index

        self.purposes_tour[timePeriod] = pivot_df
        return pivot_df
        

 
rp = ResidentPurposes(dist_eqv)
for tazSource in ["otaz", "dtaz", "hhtaz"]:
    for mapType in ["district","taz"]:
        df = rp.getResidentPurposes(mapType,'Daily',tazSource)
        df.reset_index(inplace=True,names='TAZs') 
        if mapType == "district":
            df['District'] = df['TAZs'].map(rp.distToName)
            df = modifyDistrictNameForMap(df, 'District')
        df.fillna(0,inplace=True)
        output_file = f'{mapType}_rpurpose_{tazSource[0]}.csv'
        df.to_csv(os.path.join(OUTPUT_FOLDER,output_file),index=False)

rp = ResidentPurposes(dist_eqv)
purpose_dict={}
timeperiods = ['Daily','AM','MD','PM','EV','EA']
for t in timeperiods:
    purpose_dict[t] = rp.getResidentPurposes("district",t)
purpose_df1 = purpose_dict['Daily']
purpose_df1['District'] = purpose_df1['District'].map(rp.distToName)
purpose_df1.index = purpose_df1['District']

purpose_df1.drop(['District'],axis=1,inplace=True)
sum_first_12_rows = purpose_df1.iloc[:12].sum()
sum_all_rows = purpose_df1.sum()


sum_df = pd.DataFrame([sum_all_rows,sum_first_12_rows], index=['Bay Area', 'San Francisco'])

# Concatenate the new DataFrame with the original DataFrame
df = pd.concat([sum_df, purpose_df1])
df = df.fillna(0)
df.index.name = 'Districts'
df['All Purpose'] = df.apply(lambda row: sum(row),axis=1)
column_order = [
    "Work", "School", "Escort", "Personal Business & Medical", 
    "Shopping", "Meals", "Social & Recreational", "Change Mode", 
    "Return Home", "All Purpose"
]

df_dis = df[column_order].iloc[2:14].copy()
df_dis.to_csv(os.path.join(OUTPUT_FOLDER,'purpose_all.csv'))
df = df.astype(int)
df = df.reset_index()

df2md = DataFrameToCustomHTML([0,1,14,15,16], [0])
df2md.generate_html(df, os.path.join(OUTPUT_FOLDER,"purpose_dist_daily.md"))


res = {}
for tp in timeperiods[1:]:
    temp = purpose_dict[tp]
    row_sum = temp.iloc[0:12].sum()
    res[tp] = row_sum
tod_df = pd.DataFrame(data=res).T
tod_df.drop('District',axis=1,inplace=True)
tod_df['All Purpose'] = tod_df.apply(lambda row: sum(row),axis=1)
tod_df = tod_df[column_order]

tod_df.index.name = 'TOD'
tod_df = tod_df.astype(int)
tod_df = tod_df.reset_index()
df2md = DataFrameToCustomHTML([], [0])
df2md.generate_html(tod_df, os.path.join(OUTPUT_FOLDER,"purpose_tod.md"))
tod_df.to_csv(os.path.join(OUTPUT_FOLDER,'purpose_tod.csv'))

purpose_dict={}
timeperiods = ['Daily','AM','MD','PM','EV','EA']
for t in timeperiods:
    purpose_dict[t] = rp.getResidentPurposesTour(t)
purpose_df1 = purpose_dict['Daily']
column_order_tour = ['Work','Grade School','High School','College','Escort','Personal Business(including medical)','Social & Recreational','Shopping','Meals']
purpose_df1.index = purpose_df1.index.map(rp.distToName)
purpose_df1.drop(['District'],axis=1,inplace=True)

purpose_csv = purpose_df1[column_order_tour].iloc[0:12].copy()
purpose_csv.to_csv(os.path.join(OUTPUT_FOLDER,'purpose_all_tour.csv'))

purpose_df1 = purpose_df1.astype(int)
purpose_df1.index.name = 'District'
purpose_df1 = purpose_df1.reset_index()
df2md = DataFrameToCustomHTML([], [0])
df2md.generate_html(purpose_df1, os.path.join(OUTPUT_FOLDER,"purpose_all_tour.md"))

res = {}
for tp in timeperiods[1:]:
    temp = purpose_dict[tp]
    row_sum = temp.iloc[0:12].sum()
    res[tp] = row_sum
tod_df = pd.DataFrame(data=res).T
tod_df.drop('District',axis=1,inplace=True)
tod_df.index.name = 'TOD'
tod_df = tod_df[column_order_tour].copy()
tod_df = tod_df.astype(int)
tod_df.to_csv(os.path.join(OUTPUT_FOLDER,'purpose_tod_tour.csv'))
tod_df = tod_df.reset_index()
df2md = DataFrameToCustomHTML([], [0])
df2md.generate_html(tod_df, os.path.join(OUTPUT_FOLDER,"purpose_tod_tour.md"))

os.remove(os.path.join(WORKING_FOLDER,'daysim', 'abm_output1', "trips_temp.h5"))
os.remove(os.path.join(WORKING_FOLDER,'daysim', 'abm_output1', "tours_temp.h5"))