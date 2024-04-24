
import pandas as pd
from simpledbf import Dbf5
import re
from tabulate import tabulate

LU_FILE                 = "tazdata.dbf"
LU_HHLDS                = 'Households' 
LU_POP                  = 'Population' 
LU_EMPRES               = 'Employed Residents' 
LU_TOTALEMP             = 'Total Employment' 
LU_ROWS                 = [ LU_HHLDS, LU_POP, LU_EMPRES, LU_TOTALEMP ]
ctlfile                 = 'modesumSimple3_RPM9.ctl'
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
with open(ctlfile) as f:
    ctltxt = f.read()
    m = re.search("fequiv='(.+)'", ctltxt)
    if m is None:
        exit(1)
    eqvfile = m.group(1)

# process eqvfile and save results to csv
distnames, distToTaz, tazToDist, numdists = readEqvFile(eqvfile)
res = convertToCumulative(getLanduseAttributesForDists(numdists, tazToDist))
df = pd.DataFrame.from_dict({var: list(map(int, res[var])) for var in res},
                            orient='index', columns=['Downtown', 'San Francisco', 'Bay Area'])
df.to_csv('LandUse.csv', index=True)
md_table = tabulate(df, headers='keys', tablefmt='pipe', floatfmt='.0f')
with open('land_use_table.md', 'w') as f:
    f.write(md_table)