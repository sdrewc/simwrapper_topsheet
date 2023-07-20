
import pandas as pd
from simpledbf import Dbf5
import re,os
from tabulate import tabulate
import configparser




# Read input parameters from control file
CTL_FILE                = os.environ.get('control_file')
print(CTL_FILE)
config = configparser.ConfigParser()
config.read(CTL_FILE)
WORKING_FOLDER          =  os.environ.get('input_dir')
OUTPUT_FOLDER           =  os.environ.get('output_dir')
LU_FILE_NAME            =  config['land_use']['Lu_file']
LU_CTLFILE_NAME         =  config['land_use']['LU_ctl_file']
OUTPUT_FILE_NAME        =  config['land_use']['LandUse_output_file_name']

#necessary input files for this script: "tazdata.dbf", 'modesumSimple3_RPM9.ctl', 'DIST15.eqv'
LU_FILE                 =  os.path.join(WORKING_FOLDER, LU_FILE_NAME)
LU_CTLFILE              =  os.path.join(WORKING_FOLDER, LU_CTLFILE_NAME)
LU_HHLDS                = 'Households' 
LU_POP                  = 'Population' 
LU_EMPRES               = 'Employed Residents' 
LU_TOTALEMP             = 'Total Employment' 
LU_ROWS                 = [ LU_HHLDS, LU_POP, LU_EMPRES, LU_TOTALEMP ]
# eqvfile               = 'DIST15.eqv'

#1. It's imported from summit file.
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

def getLanduseAttributesForDists(numdists,tazToDist):
    """ Reads the landuse file (LU_FILE) and returns
        landuse: LU_HHLDS, LU_POP, ... -> list of nums for each district
    """
    landuse     = { LU_HHLDS:[], LU_POP:[], LU_EMPRES:[], LU_TOTALEMP:[] }
    for var in landuse:
        for dist in range(0,numdists):
            landuse[var]=[0 for _ in range(numdists)]
    dbf = Dbf5(LU_FILE)
    df = dbf.to_dataframe()
    for index, row in df.iterrows():
        taz = row["SFTAZ"]
        if (taz not in tazToDist.keys()):
            print("Taz " + str(taz) + " not part of any district")
        else:
            for dist in tazToDist[taz]:
                # make strings constants... there are vars already 
                landuse[LU_HHLDS][dist-1]   = landuse[LU_HHLDS][dist-1]     + row['HHLDS']
                landuse[LU_POP][dist-1]     = landuse[LU_POP][dist-1]       + row['POP']
                landuse[LU_EMPRES][dist-1]  = landuse[LU_EMPRES][dist-1]    + row['EMPRES']
                landuse[LU_TOTALEMP][dist-1]= landuse[LU_TOTALEMP][dist-1]  + row['TOTALEMP']
    return landuse

def convertToCumulative(thedict):
    """ Given a dict of attrib -> dist1, dist2, dist3, ...
        Converts to cumulative dict of
        attrib -> dist1, dist1+dist2, dist1+dist2+dist3, ... 
    """
    cumdict = {}
    for attrib in thedict.keys():
        cumdict[attrib] = []
        cumulate = 0
        for ind in range(0, len(thedict[attrib])):
            cumulate = cumulate + thedict[attrib][ind]
            cumdict[attrib].append(cumulate)
    return cumdict


# read ctlfile and extract eqvfile path
with open(LU_CTLFILE) as f:
    ctltxt = f.read()
    m = re.search("fequiv='(.+)'", ctltxt)
    if m is None:
        exit(1)
    eqvfile = m.group(1)
eqvfile_path = os.path.join(WORKING_FOLDER , eqvfile)
# process eqvfile and save results to csv
distnames, distToTaz, tazToDist, numdists = readEqvFile(eqvfile_path)
res = convertToCumulative(getLanduseAttributesForDists(numdists,tazToDist))
index = []
dt = []
sf = []
bay = []
for var in res:
    index.append(var)
    dt.append(int(res[var][0]))
    sf.append(int(res[var][1]))
    bay.append(int(res[var][2]))
df = pd.DataFrame(data = {'Downtown': dt, 'San Francisco': sf, 'Bay Area': bay},index = index)
csv_df = df
csv_df.index.name = 'Area'
csv_df = csv_df.reset_index().melt(id_vars=['Area'], var_name='category', value_name='value')


csv_path = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}.csv')
csv_df.to_csv(csv_path, index=False)
df.index.name = 'Category'
md_path = os.path.join(OUTPUT_FOLDER, f'{OUTPUT_FILE_NAME}.md')
# md_table = tabulate(df, headers='keys', tablefmt='pipe', floatfmt='.0f')
markdown_table = tabulate(df, headers='keys', tablefmt='pipe', floatfmt='.0f').split('\n')
markdown_table[0] = '| ' + ' | '.join(f'**{header.strip()}**' for header in markdown_table[0].split('|')[1:-1]) + ' |'
markdown_table[1] = markdown_table[1].replace('|', ':|:').replace('::', ':')[1:-1]
markdown_table = '\n'.join(markdown_table)
with open(md_path, 'w') as f:
    f.write(markdown_table)