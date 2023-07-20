import numpy
import pandas as pd
import datetime,os,re,sys,subprocess
from socket import gethostname
import configparser

from simpledbf import Dbf5 as dbf
import datetime,os,re,sys,subprocess
from tables import open_file

def runtpp(scriptname, outputfile, env=None):
    """ Runs the given tpp script if the given outputfile doesn't exist
    """
    try:
        if (runAll):
            raise Exception('runAll', 'runAll')
        else:
            os.stat(outputfile)
    except:
        # it doesn't exist, run it
        hostname = gethostname()
        fullenv = None
        if env:
            fullenv = os.environ
            fullenv.update(env)
        # dispatch it, no cube license
        if (hostname == 'Mason' or hostname == 'D90V07K1-okforgetit'):
            f = open('topsheet.tmp', 'w')
            f.write("runtpp " + scriptname + "\n")
            f.close()
            scriptname = scriptname.replace('/', '\\')
            dproc    = subprocess.Popen( "Y:/champ/util/bin/dispatch.bat topsheet.tmp ocean", env=fullenv) 
            dret     = dproc.wait()
        else:
            dproc    = subprocess.Popen( "runtpp " + scriptname, env=fullenv)    
            dret     = dproc.wait()
        print("dret = ",dret)
    return

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
			if (dist not in distToTaz):
				distToTaz[dist] = []              
			distToTaz[dist].append(taz)
			if (taz not in tazToDist):
				tazToDist[taz] = []
			tazToDist[taz].append(dist)
			if (m.group(3) != None):
				distnames[dist] = m.group(3).strip(' ')
		lineno	= lineno + 1
	numdists	= len(distnames)
	return (distnames, distToTaz, tazToDist, numdists) 
  
#newly added
def readCtlFile(ctlfile, runsummit=False):
    config = configparser.ConfigParser()
    config.read(ctlfile)
    return config

#----------------------------------------------------Resident Purpose--------------------------------------------------
rp_params = readCtlFile(os.environ.get('control_file')) #newly added
input_folder = os.environ.get('input_dir')
output_folder = os.environ.get('output_dir')
rp_hh = os.path.join(input_folder, rp_params['resident_purpose']['RP_HH'])
rp_person = os.path.join(input_folder, rp_params['resident_purpose']['RP_PERSON'])
rp_tour = os.path.join(input_folder, rp_params['resident_purpose']['RP_TOUR'])
rp_trips = os.path.join(input_folder, rp_params['resident_purpose']['RP_DISAG_TRIPS'])
rp_rows = rp_params['resident_purpose']['RP_ROWS'].split(',')
trips_file = os.path.join(input_folder, rp_params['resident_purpose']['trips_file'])
eqv_file = os.path.join(input_folder, rp_params['resident_purpose']['eqv_file'])
time_periods = rp_params['resident_purpose']['time_periods'].split(',')

print("Getting the inputs from: "+input_folder)

class ResidentPurposes:
    RP_HH           = rp_hh
    RP_PERSON       = rp_person
    RP_TOUR         = rp_tour
    RP_DISAG_TRIPS  = rp_trips
    RP_ROWS         = rp_rows
    #Can we change the row order? grade, high, college, work, workbased and other?
    RP_PURPOSES     = { 1:RP_ROWS[0], 2:RP_ROWS[1], 3:RP_ROWS[2], 4:RP_ROWS[3], 5:RP_ROWS[4], 6:RP_ROWS[5] }
    
    def __init__(self, eqvfile):
        #What is this? from champUtil?
        (self.distToName, self.distToTaz, self.tazToDist, _numdists ) = readEqvFile(eqvfile)
        self.purposesByTimeperiod = False
    
    def getChampPurpose(self, row, col_dict):
        #Unable to identify input row and col_dict
        #could change it into pandas...
        tour_parent = row[col_dict['parent']]
        ptype = row[col_dict['pptyp']]
        xfer_purp = row[col_dict['dpurp']]
        if tour_parent>0:
            return ResidentPurposes.RP_ROWS[5] # Workbased
        elif xfer_purp == 1:
            return ResidentPurposes.RP_ROWS[0] # Work
        elif xfer_purp == 2:
            if ptype == 7: # child age 5-15
                return ResidentPurposes.RP_ROWS[1] # Grade School
            elif ptype == 6: # child age 16+
                return ResidentPurposes.RP_ROWS[2] # High School
            else: 
                return ResidentPurposes.RP_ROWS[3] # College
        elif xfer_purp == 3:
            return ResidentPurposes.RP_ROWS[6] #Escorts
        elif xfer_purp == 4:
            return ResidentPurposes.RP_ROWS[7] #Personal business
        elif xfer_purp == 5:
            return ResidentPurposes.RP_ROWS[9] #Shopping
        elif xfer_purp == 6:
            return ResidentPurposes.RP_ROWS[10] # Meals
        elif xfer_purp == 7:
            return ResidentPurposes.RP_ROWS[8] #Social
        elif xfer_purp == 10:
            return None
        else:
            return ResidentPurposes.RP_ROWS[4] # Other
    
    def getTimePeriod(self, row, col_dict):
        '''EA = early monring(3am till 6am), AM = monring a.m. peak(6am till 9am), 
        MD = midday(9am till 3.30pm), PM = p.m peak(3.30pm till 6.30pm) and EV = evening(6.30pm till 3am)
         ---- Time is given in minutes'''
        seg_dir = row[col_dict['half']]
        dep_time = row[col_dict['deptm']]
        arr_time = row[col_dict['arrtm']]
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
    
    def getResidentPurposes(self, rundir, timePeriod):
        """
        *rundir* is where we'll read the TRIPMC file
        *timePeriod* can be one of "Daily", "AM" or "PM"
        
        Returns dictionary of purpose (Work,School,etc) -> 
           list of trips for that purpose for each district
        """
        # if it's done already, return it
        if self.purposesByTimeperiod:
            # print("Check")
            return self.purposesByTimeperiod[timePeriod]
        
        # otherwise get it
        trip_df = pd.read_csv(ResidentPurposes.RP_DISAG_TRIPS, sep='\t', 
                              usecols = ['hhno','pno','tour_id','dpurp','half','deptm','arrtm'])
        tour_df = pd.read_csv(ResidentPurposes.RP_TOUR, sep='\t', usecols = ['id','parent'])
        hh_df   = pd.read_csv(ResidentPurposes.RP_HH, sep='\t', usecols = ['hhno','hhtaz'])
        per_df  = pd.read_csv(ResidentPurposes.RP_PERSON, sep='\t', usecols = ['hhno','pno','pptyp'])
        
        trip_df = trip_df.merge(hh_df, how='left')
        trip_df = trip_df.merge(per_df, how='left')
        trip_df = trip_df.merge(tour_df, how='left', left_on='tour_id', right_on='id')
        store = pd.HDFStore(trips_file, 'w')
        store.put('root', trip_df, format='t')
        store.close()
        # print(trip_df)
        
        self.purposesByTimeperiod = { "Daily":{}, "AM":{}, "PM":{}}
        for tp in self.purposesByTimeperiod.keys():
            for purpose in  ResidentPurposes.RP_ROWS:
                if purpose == "Total": continue
                self.purposesByTimeperiod[tp][purpose] = [0,0,0]

        #Can use pandas or h5py instead of tables to do this processing
        infile = open_file(trips_file, mode="r")
        col_names = infile.get_node('/root', 'table')._v_attrs.values_block_0_kind
        col_idx_dict = dict([(col,i) for col,i in zip(col_names, list(range(len(col_names))))])        
#         print strftime("%x %X", localtime()) + " started Resident Purposes processing"
        for row in infile.get_node('/root', 'table'):
            row = row[1]
            # print((self.tazToDist[row[col_idx_dict['hhtaz']]]))
            resdist = int(self.tazToDist[row[col_idx_dict['hhtaz']]][0])
            purpose = self.getChampPurpose(row, col_idx_dict) #what if we pass the whole df and get the whole result together??
            if purpose != None:
                timeperiod = self.getTimePeriod(row, col_idx_dict)
                 
                if timeperiod == "AM" or timeperiod == "PM":
                    self.purposesByTimeperiod[timeperiod][purpose][resdist-1] += 1
                 
                self.purposesByTimeperiod["Daily"][purpose][resdist-1] += 1


#         print strftime("%x %X", localtime()) + " completed."
        # print self.purposesByTimeperiod[timePeriod]

        infile.close()
        print("Done!")
        return self.purposesByTimeperiod[timePeriod]



def getResidentPurposeFiles(eqv, loc, time = ['Daily']):
    """
    Write the resident purpose dictionary into csv and markdown files
    """
    rp = ResidentPurposes(eqv)

    for t in time:
        file_name = 'purpose_'+t
        # print(file_name)
        purpose_dict = rp.getResidentPurposes('./',t)
        purpose_df = pd.DataFrame(data = purpose_dict)
        purpose_df = purpose_df.transpose().reset_index()
        purpose_df.columns = ['Purpose','Downtown','San Francisco','Bay Area']
        # purpose_df1=purpose_df.copy()
        purpose_df['San Francisco'] = purpose_df['Downtown']+purpose_df['San Francisco']
        purpose_df['Bay Area'] = purpose_df['Bay Area']+purpose_df['San Francisco']
        
        purpose_df.drop(4,inplace=True) # removing Others
        # purpose_df1.drop(4,inplace=True) # removing Others

        total_row = ['Total',purpose_df['Downtown'].sum(),purpose_df['San Francisco'].sum(),purpose_df['Bay Area'].sum()]
        purpose_df.loc[11] = total_row
        purpose_df['Downtown %'] = purpose_df['Downtown']/total_row[1]
        purpose_df['SF %'] = purpose_df['San Francisco']/total_row[2]
        purpose_df['Bay %'] = purpose_df['Bay Area']/total_row[3]
        purpose_df['Downtown']=purpose_df.apply(lambda x: "{:,}".format(x['Downtown']), axis=1)
        purpose_df['San Francisco']=purpose_df.apply(lambda x: "{:,}".format(x['San Francisco']), axis=1)
        purpose_df['Bay Area']=purpose_df.apply(lambda x: "{:,}".format(x['Bay Area']), axis=1)

        csvfile_path = os.path.join(loc, file_name + '.csv')
        #CSV table
        purpose_df2=purpose_df.copy()
        purpose_df2.drop(11,inplace=True)
        purpose_df2.to_csv(csvfile_path, sep='\t',index=False)

        #Markdown table
        markdown_table = purpose_df.to_markdown(index=False).split('\n')
        header_row = markdown_table[0]
        header_row = '| ' + ' | '.join(f'**{header.strip()}**' for header in header_row.split('|')[1:-1]) + ' |'
        markdown_table[0] = header_row
        markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::',':')[1:-1]
        markdown_table = '\n'.join(markdown_table)

        # print(markdown_table)
        mdfile_path = os.path.join(loc, file_name + '.md')
        # print(mdfile_path)
        with open(mdfile_path, 'w') as f:
            f.write(markdown_table)
        
        print(f"Both files for {t} are finished writing!!!!")
            
    
    return "Resident Purpose files are written in " + loc
    

getResidentPurposeFiles(eqv = eqv_file, loc = output_folder, time = time_periods)