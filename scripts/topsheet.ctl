# 
[folder_setting]

necessary_scripts_for_vmt_folder = Y:/Users/Patricie/champ/champ/scripts/summarize

# part 1 Landuse table

[land_use]

Lu_file                          = tazdata.dbf
LU_ctl_file                      = modesumSimple3_RPM9.ctl
LandUse_output_file_name         = Landuse

#part2 Mode share table

[mode_share]

MS_SUMMIT_RPM9_CTL               = modesumSimple3_RPM9.ctl
MS_SUMMIT_CHAMP_CTL              =  modesumSimple3_Champ.ctl
MS_eqvfile                       = DISTSIMPLE3.eqv
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
Modeshare_output_file_name       = Mode_share

#part3 VMT_VHT table

[vmt_vht]

VMT_LOADEXPORT_CMD               = create-daily.s
VMT_GAWK_SCRIPT                  = vmt_vht_summary.awk
AM_vmt_vht_file                  = LOADAM_FINAL.txt
PM_vmt_vht_file                  = LOADPM_FINAL.txt
MD_vmt_vht_file                  = LOADMD_FINAL.txt
EV_vmt_vht_file                  = LOADEV_FINAL.txt
EA_vmt_vht_file                  = LOADEA_FINAL.txt
vmt_vht_output_file_name         = VMT_VHT

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
Transit_File_Name                = Transit

#part 5 Traffic

[traffic]
daily_vols_dbf                   = daily_vols.dbf
am_vols_dbf                      = am_vols.dbf
pm_vols_dbf                      = pm_vols.dbf
Traffic_LOADEXPORT_CMD           = create-daily.s
Trffic_File_Name                 = Traffic

# Part 6 Resident Purpose

[resident_purpose]


RP_HH           = _household_2.dat
RP_PERSON       = _person_2.dat
RP_TOUR         = _tour_2.dat
RP_DISAG_TRIPS  = _trip_2.dat
RP_ROWS         = Work,Grade School,High School, College,Other,Workbased,Escort,Personal Business(including medical),Social & Recreational,Shopping,Meals,Total
trips_file = trips.h5
eqv_file = DISTSIMPLE3.eqv
time_periods = Daily,AM,PM
