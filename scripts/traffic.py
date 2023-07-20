import numpy as np
import pandas as pd
import datetime,os,re,sys,subprocess
from socket import gethostname

from simpledbf import Dbf5 as dbf
import datetime,os,re,sys,subprocess
from tables import open_file
import configparser




# Read input parameters from control file
CTL_FILE                = os.environ.get('control_file')
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  os.environ.get('input_dir')
OUTPUT_FOLDER           =  os.environ.get('output_dir')
# Access the arguments
daily_vols_dbf_path     = os.path.join(WORKING_FOLDER, config['traffic']['daily_vols_dbf'])
am_vols_dbf_path        = os.path.join(WORKING_FOLDER, config['traffic']['am_vols_dbf'])
pm_vols_dbf_path        = os.path.join(WORKING_FOLDER, config['traffic']['pm_vols_dbf'])
necessary_scripts_for_vmt_folder    =  config['folder_setting']['necessary_scripts_for_vmt_folder']
output_filename         = config['traffic']['Trffic_File_Name']
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
		m 	= distline_re.search(lines[lineno])
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


#Traffic constants
# SCRIPT_PATH             = sys.path[0] + "\\"
HWY_CREATEDAILY_CMD     = os.path.join(necessary_scripts_for_vmt_folder, config['traffic']['Traffic_LOADEXPORT_CMD'])
HWY_VOLFILES            = { 'AM':am_vols_dbf_path, 'MD':'md_vols.dbf', 'PM':pm_vols_dbf_path, 
                            'EV':'ev_vols.dbf', 'EA':'ea_vols.dbf', 'Daily':daily_vols_dbf_path }
HWY_ALLCOUNTYLINES      = 'All SF County Lines'
HWY_ROWS                = [ HWY_ALLCOUNTYLINES, 'SM County Line', 'Bridges', ['Bay Bridge',1], ['GG Bridge',1], 
                            ['San Rafael Bridge (WB,EB)',1], ['San Mateo Bridge (WB,EB)',1], ['Dumbarton Bridge (WB,EB)',1],
                            [ 'Carquinez Bridge',1], # ['Benicia-Martinez Bridge',1],
                            'Octavia @ Market (NB,SB)', '19th Ave @ Lincoln (NB,SB)',
                            'Geary/O\'Farrell @ VN (EB,WB)', 'Van Ness @ Geary (NB,SB)' ]
HWY_SCREENS             = { \
    "Bay Bridge":[
                    [ 52832, 52494 ],   # inbound
                    [ 52495, 52833 ],   # outbound
    ],
    "GG Bridge":[
                    [ 52267, 52425 ],   # inbound
                    [ 52426, 52268 ],   # outbound
    ],
    "San Rafael Bridge (WB,EB)":[
                    [ 103342, 8894 ],     # inbound = westbound
                    [ 8853, 103341 ],     # outbound = eastbound
    ],
    "San Mateo Bridge (WB,EB)":[
                    [ 7381, 7393 ],     # inbound = westbound
                    [ 7382, 7383 ],     # outbound = eastbound
    ],
    "Dumbarton Bridge (WB,EB)":[
                    [ 6938, 6921 ],     # inbound = westbound
                    [ 6922, 6939 ],     # outbound = eastbound
    ],
    "Carquinez Bridge":[
                    [ 12643, 12667, 
                      12643, 12634 ],   # inbound
                    [ 12637, 12642,
                      10482, 10483 ],   # outbound
    ],
    "Benicia-Martinez Bridge":[
                    [ 3131, 3136,
                      3131, 10247 ],    # inbound
                    [ 3135, 3130,
                      3132, 3142 ],     # outbound
    ],
    "SM County Line":[ # done by looking @ network and loading bayarea_county.shhp
                       # just documenting all cross points
                       # the big 3 (101, 280, 35) all match the roadway validation spreadsheet
                    [ 7678,  51171,     # inbound - 35 
                      7677,  33539,     # lake merced
                      51450, 33467,     # junipero serra
                      51130, 22527,     # saint charles
                      52270, 22515,     # de long
                      52234, 52271,     # 280 north
                      51137, 22513,     # santa cruz
                      52456, 51138,     # santa barbara
                      33704, 33454,     # shakespeare st
                      33704, 22482,     # rhine st
                      51142, 22482,     # flournoy st
                      51113, 22479,     # wilson st
                      51113, 22464,     # san jose ave
                      52461, 22464,     # goethe st
                      52462, 22465,     # rice st
                      52463, 22452,     # liebig st
                      52787, 21584,     # mission st
                      52792, 33702,     # irvington st
                      52469, 33702,     # acton st
                      52459, 21531,     # brunswick st
                      52790, 21531,     # oliver st
                      51019, 21500,     # whittier st
                      51019, 21493,     # hanover st
                      52774, 21493,     # lowell st
                      33603, 21461,     # guttenberg st
                      33844, 33351,     # pope st
                      51000, 33351,     # polaris way
                      52487, 51009,     # cordova st
                      52487, 21371,     # canyon way
                      51002, 21371,     # south hill blvd
                      52489, 21359,     # carter st
                      52482, 20421,     # geneva ave
                      52482, 20413,     # pasadena st
                      52483, 20412,     # castillo st
                      52481, 20411,     # pueblo str
                      52484, 20402,     # calgary st
                      52485, 20403,     # rio verde st
                      33605, 20373,     # velasco ave
                      33605, 20375,     # schwerin st
                      50995, 20306,     # bayshore
                      6986,  50856,     # tunnel ave
                      6985,  52492,     # alana way
                      40029, 52118,     # hwy 101nb
                      40003, 50852 ],   # harney way
                    [ 51171, 7678,      # outbound - 35
                      33549, 7677,      # lake merced
                      22528, 51449,     # alemany blvd
                      52480, 51449,     # junipero serra
                      22527, 51130,     # saint charles
                      52137, 52236,     # 280 s offramp
                      52137, 52235,     # another 280 s offramp
                      22515, 52270,     # de long
                      22513, 51137,     # santa cruz
                      51138, 52456,     # santa barbara
                      33454, 33704,     # shakespeare st
                      22482, 33704,     # rhine st
                      22482, 51142,     # flournoy st
                      22479, 51113,     # wilson st
                      22464, 51113,     # san jose ave
                      22464, 52461,     # goethe st
                      22465, 52462,     # rice st
                      22452, 52463,     # liebig st
                      21584, 52787,     # mission st
                      33702, 52792,     # irvington st
                      33702, 52469,     # acton st
                      21531, 52459,     # brunswick st
                      21531, 52790,     # oliver st
                      21500, 51019,     # whittier st
                      21493, 51019,     # hanover st
                      21493, 52774,     # lowell st
                      21461, 33603,     # guttenberg st
                      33351, 33844,     # pope st
                      33351, 51000,     # polaris way
                      51009, 52487,     # cordova st
                      21371, 52487,     # canyon way
                      21371, 51002,     # south hill blvd
                      21359, 52489,     # carter st
                      20421, 52482,     # geneva ave
                      20413, 52482,     # pasadena st
                      20412, 52483,     # castillo st
                      20411, 52481,     # pueblo str
                      20402, 52484,     # calgary st
                      20403, 52485,     # rio verde st
                      20373, 33605,     # velasco ave
                      20375, 33605,     # schwerin st
                      20306, 50995,     # bayshore
                      50856, 6986,      # tunnel ave
                      52492, 6985,      # alana way
                      52264, 7732,      # hwy 101sb
                      69013, 10689,		# hwy 101sb HOV
                      50852, 40003 ]    # harney way
    ],
    "Octavia @ Market (NB,SB)":[
                    [ 30756, 25858,     # inbound
                      30756, 53029 ],
                    [ 53029, 30756,     # oubound
                      53028, 30756 ]
    ],
    "Geary/O'Farrell @ VN (EB,WB)":[
                    [ 25196, 25192 ],   # inbound
                    [ 25197, 25213 ],   # outbound
    ],
    "Van Ness @ Geary (NB,SB)":[
                    [ 25195, 25197 ],   # nb
                    [ 25197, 30709 ],   # sb
    ],
    "19th Ave @ Lincoln (NB,SB)":[
                    [ 27372, 27374 ],   # nb
                    [ 27374, 27372 ],   # sb
    ],
}   


def getTrafficScreens(timePeriod='Daily'):
    """
    """
    hwyfile     = HWY_VOLFILES[timePeriod]
    # print(hwyfile)
    runtpp(HWY_CREATEDAILY_CMD, hwyfile)    # create it, if needed
    file_dbf = dbf(hwyfile)#to be changed later!!!!!!!!!!!
    hwydbf      = file_dbf.to_dataframe()
    abToScreen  = {}
    traffic     = {}
    vol         = 'TOTVOL'
    if (timePeriod == 'Daily'):
        vol     = 'DAILY_TOT'
    # build a reverse mapping for what we want
    ## We can use numpy or direct for key,value in dict.itmes() instead of so many loops
    for key,value in HWY_SCREENS.items():
        #inbound
        for index in range(0,len(value[0]),2):
            abToScreen[str(value[0][index])+" "+str(value[0][index+1])] = [ key, 0 ]
        #outbound
        for index in range(0,len(value[1]),2):
            abToScreen[str(value[1][index])+" "+str(value[1][index+1])] = [ key, 1 ]

    # print(abToScreen)
    # print(hwydbf)
    for i in range(len(hwydbf)):
        rec = hwydbf.loc[i]
        if (rec['AB'] in abToScreen):
            screen,inout = abToScreen[rec['AB']][0],abToScreen[rec['AB']][1]
            if screen not in traffic: traffic[screen] = [ 0, 0 ]
            # if (screen == 'SM County Line'):
            #   print screen, inout, rec['AB'], int(rec[vol])
            traffic[screen][inout] = traffic[screen][inout] + rec[vol]
    # make them ints
    for screen in traffic:
        traffic[screen][0] = int(traffic[screen][0])
        traffic[screen][1] = int(traffic[screen][1])

    # aggregate
    traffic[HWY_ALLCOUNTYLINES] = [ 0, 0]
    traffic[HWY_ALLCOUNTYLINES][0] = \
         traffic['SM County Line'][0] + traffic['Bay Bridge'][0] + traffic['GG Bridge'][0]
    traffic[HWY_ALLCOUNTYLINES][1] = \
         traffic['SM County Line'][1] + traffic['Bay Bridge'][1] + traffic['GG Bridge'][1]
    return traffic

def format_cols(x):
    return re.sub(r'\([^)]*\)', '',x)

def getTrafficFiles(loc, time = ['Daily','AM','PM']):
    """
    Write the traffic data into csv and markdown files
    """
    for t in time:
        file_name = f'{output_filename}_'+t
        # print(file_name)
        dict_screen = getTrafficScreens(t)
        screen_df = pd.DataFrame(data=dict_screen)
        traffic_df = screen_df.transpose().reset_index()
        index_order = [10,9,1,8,7,6,5,0,4,2,3,11]
        traffic_df = traffic_df.reindex(index_order,  axis=0)
        traffic_df.columns = ['Lines','Inbound','Outbound']
        csv_df = traffic_df.copy()
        csv_df.drop(11,inplace=True)
        csv_df['Inbound']=csv_df.apply(lambda x: "{:,}".format(x['Inbound']), axis=1)
        csv_df['Outbound']=csv_df.apply(lambda x: "{:,}".format(x['Outbound']), axis=1)
        csv_df['Lines']=csv_df['Lines'].apply(format_cols)
        # traffic_df=traffic_df.sort_values(by='Outbound',ascending=False)
        #CSV table
        csv_df.to_csv(os.path.join(OUTPUT_FOLDER, file_name+'.csv'),sep='\t',index=False)
        #Markdown table
        markdown_table = csv_df.to_markdown(index=False).split('\n')
        header_row = markdown_table[0]
        header_row = '| ' + ' | '.join(f'**{header.strip()}**' for header in header_row.split('|')[1:-1]) + ' |'
        markdown_table[0] = header_row
        markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::',':')[1:-1]
        markdown_table = '\n'.join(markdown_table)

        # print(markdown_table)
        
        with open(os.path.join(OUTPUT_FOLDER, file_name+'.md'), 'w') as f:
            f.write(markdown_table)
            
    return "Traffic summary files are written in " + loc


if __name__ == '__main__':
    getTrafficFiles(loc = OUTPUT_FOLDER, time = ['Daily','AM','PM'])