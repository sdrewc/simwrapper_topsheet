import configparser
import re

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

def readCtlFile(ctlfile, runsummit=False):
    config = configparser.ConfigParser()
    config.read(ctlfile)
    return config