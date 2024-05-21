# 
[folder_setting]


WORKING_FOLDER                   =  X:\Projects\CHAMP7\Run25t_Market
OUTPUT_FOLDER                    =  X:\Projects\Miscellaneous\topsheet_simwrapper\test_final\simwrapper_topsheet\data

SCRIPT_FOLDER                    =  Y:\Users\Wei\scripts
# part 1 Landuse table

[land_use]

LU_FILE                          = tazdata.dbf
LU_CLEAN_FILE                    = TAZ2454.dbf
LU_CTL_FILE                      = modesumSimple3_RPM9.ctl
DIST_EQV                         = DIST15.eqv
AREA_EQV                         = DISTSIMPLE3.eqv



#part2 Mode share table

[mode_share]

MS_SUMMIT_RPM9_CTL               = modesumSimple3_RPM9.ctl
MS_SUMMIT_CHAMP_CTL              =  modesumSimple3_Champ.ctl
DIST_EQV                         = DIST15.eqv
AREA_EQV                         = DISTSIMPLE3.eqv
AM_h5_file                       =  PERSONTRIPS_AM.h5
MD_h5_file                       =  PERSONTRIPS_MD.h5
PM_h5_file                       =  PERSONTRIPS_PM.h5
EV_h5_file                       =  PERSONTRIPS_ev.h5
EA_h5_file                       =  PERSONTRIPS_ea.h5
AM_mat_file                      =  PERSONTRIPS_AM.MAT
PM_mat_file                      =  PERSONTRIPS_PM.MAT
EV_mat_file                      =  PERSONTRIPS_EV.MAT
EA_mat_file                      =  PERSONTRIPS_EA.MAT
MD_mat_file                      =  PERSONTRIPS_MD.MAT
Convert_mat2h5                   =  mat2h5.exe
RP_DISAG_TRIPS                   = _trip_2.dat


#part3 VMT_VHT table

[vmt_vht]

VMT_LOADEXPORT_CMD               = create-daily.s
NET2CSV                          = NETtoCSV_TNC.s
AM_vmt_vht_file                  = LOADAM_FINAL
PM_vmt_vht_file                  = LOADPM_FINAL
MD_vmt_vht_file                  = LOADMD_FINAL
EV_vmt_vht_file                  = LOADEV_FINAL
EA_vmt_vht_file                  = LOADEA_FINAL


#part4 Transit

[transit]
SFALLMSAAM_DBF                   = SFALLMSAAM.DBF
SFALLMSAPM_DBF                   = SFALLMSAPM.DBF
SFALLMSAMD_DBF                   = SFALLMSAMD.DBF
SFALLMSAEV_DBF                   = SFALLMSAEV.DBF
SFALLMSAEA_DBF                   = SFALLMSAEA.DBF
SFALLMSAAM_CSV                   = SFALLMSAAM.csv
SFALLMSAPM_CSV                   = SFALLMSAPM.csv
SFALLMSAMD_CSV                   = SFALLMSAMD.csv
SFALLMSAEV_CSV                   = SFALLMSAEV.csv
SFALLMSAEA_CSV                   = SFALLMSAEA.csv
FREEFLOW_nodes_DBF               = FREEFLOW_nodes.DBF
LINKEDMUNI_AM_DBF                = LINKEDMUNI_AM.DBF
LINKEDMUNI_PM_DBF                = LINKEDMUNI_PM.DBF
LINKEDMUNI_MD_DBF                = LINKEDMUNI_MD.DBF
LINKEDMUNI_EV_DBF                = LINKEDMUNI_EV.DBF
LINKEDMUNI_EA_DBF                = LINKEDMUNI_EA.DBF
VALIDATE_CSV                     = cubenet_validate_nodes.csv


#part 5 Traffic

[traffic]
daily_vols_dbf                   = daily_vols.dbf
am_vols_dbf                      = am_vols.dbf
pm_vols_dbf                      = pm_vols.dbf
md_vols_dbf                      = md_vols.dbf
ev_vols_dbf                      = ev_vols.dbf
ea_vols_dbf                      = ea_vols.dbf
Traffic_LOADEXPORT_CMD           = create-daily.s

# Part 6 Resident Purpose

[resident_purpose]


RP_HH                            = _household_2.dat
RP_PERSON                        = _person_2.dat
RP_TOUR                          = _tour_2.dat
RP_DISAG_TRIPS                   = _trip_2.dat
DIST_EQV                         = DIST15.eqv
AREA_EQV                         = DISTSIMPLE3.eqv
