#!/usr/bin/env python
# run_viirs2ioda.py
# process VIIRS files and produce JEDI/IODA compatible obs files
import os
import subprocess as sp
import datetime as dt
import glob
import sys 
#import xarray as xr

#grid='96'
grid='192'

InPath='/scratch2/NCEPDEV/stmp3/Andrew.Tangborn/VIIRS/CLASS/'
OutPath='/scratch2/NCEPDEV/stmp3/Andrew.Tangborn/VIIRS/'
#FV3Grid='/scratch1/BMC/gsd-fv3-dev/MAPP_2018/pagowski/fix_fv3/C'+grid
CycleHrs=6
thinning = 0.9999
year=2019
month=6
day=27
hour=18
yymmdd_int = str(year)+str(month).zfill(2)+str(day).zfill(2)
InRoot = InPath + yymmdd_int
OutRoot1 = OutPath +'thin'+str(thinning)+'/'  
if not os.path.exists(OutRoot1):
   os.makedirs(OutRoot1)
OutRoot = OutRoot1 + yymmdd_int
if not os.path.exists(OutRoot):
   os.makedirs(OutRoot)
date1 = dt.date(year,month,day)
print('InRoot=',InRoot)
yyyymmddhh = int(str(year)+str(month).zfill(2)+str(day).zfill(2)+str(hour).zfill(2))
print('yyyymmddhh =',yyyymmddhh)
InRoot2 = InPath + yymmdd_int
if hour==0:
   date2 = dt.timedelta(days=-1)  + date1
   yyyymmdd2 = date2.strftime("%Y%m%d")
   print('yyyymmdd2=',yyyymmdd2)
   InRoot2 = InPath + yyyymmdd2


#year=2022
#month=4
#day=4
#hour=0
#yyyymmddhh = 2022040400
StartCycle=dt.datetime(year,month,day,hour)
EndCycle=dt.datetime(year,month,day,hour)
#print('StartCycle=',StartCycle)


#executable='python /scratch1/NCEPDEV/da/Andrew.Tangborn/JEDI/jan27_aod2ioda/build/bin/viirs_aod2ioda.py'
executable='python /scratch1/NCEPDEV/da/Andrew.Tangborn/JEDI/gdas_app_august5_2022/gdasapp/build/bin/viirs_aod2ioda.py' 
my_env = os.environ.copy()
my_env['OMP_NUM_THREADS'] = '4' # for openmp to speed up fortran call
#./viirs2ioda.x $validtime $fv3dir $infile $outfile

HalfCycle = CycleHrs/2
NowCycle=StartCycle

while NowCycle <= EndCycle:
#  print("Processing analysis cycle: "+NowCycle.strftime("%Y-%m-%d_%H:%M UTC"))

  # get +- half of cycle hours
  StartObs = NowCycle - dt.timedelta(hours=HalfCycle)
  EndObs = NowCycle + dt.timedelta(hours=HalfCycle)
  StartObs_doy = StartObs.timetuple().tm_yday
  EndObs_doy = EndObs.timetuple().tm_yday 
  EndObs_mon = EndObs.timetuple().tm_mon
  EndObs_mday = EndObs.timetuple().tm_mday
  StartObs_mon = StartObs.timetuple().tm_mon
  StartObs_mday = StartObs.timetuple().tm_mday 
# These times are not matched up with the root directory.
# Fix this next 
  print('StartObs = ', StartObs)
  print('EndObs_mon = ', EndObs_mon)
  print('EndObs_doy = ', EndObs_doy)
  print('EndObs_mday = ', EndObs_mday) 
  
  str_start_mon = str(StartObs_mon)
  str_start_mon = str_start_mon.zfill(2)
  str_start_mday = str(StartObs_mday)
  str_start_mday = str_start_mday.zfill(2) 
  str_year = str(StartObs.year)
  str_end = str(EndObs_doy)
  str_end_mon = str(EndObs_mon)
  str_end_mon = str_end_mon.zfill(2)
  str_end_mday = str(EndObs_mday)
  str_end_mday = str_end_mday.zfill(2)

  print('str_start_mon =', str_start_mon)  
  print('str_start_mday =', str_start_mday)
  print('str_end_mon =', str_end_mon)
  print('str_end_mday = ', str_end_mday) 
#  str_start = 'JRR-AOD_v2r0_j01_s'+str_year+str_start_mon+str_start_mday
#  str_end =  'JRR-AOD_v2r0_j01_s'+str_year+str_end_mon+str_end_mday
  str_start = 'JRR-AOD_v2r0_npp_s'+str_year+str_start_mon+str_start_mday
  str_end = 'JRR-AOD_v2r0_npp_s'+str_year+str_end_mon+str_end_mday
  print('str_start = ', str_start)
  print('str_end = ', str_end) 
  # get possible files to use
  usefiles = []
  dir1 = InRoot2+'/'+str_start
  dir2 = InRoot+'/'+str_end
#  dir3 = InRoot2+'/'+str_start1
#  dir4 = InRoot+'/'+str_end1
  if hour==0:
     dir5 = InRoot2+'/'+str_start
     dir6 = InRoot+'/'+str_end
#     dir7 = InRoot2+'/'+str_start1
#     dir8 = InRoot+'/'+str_end1
    
  print('dir1=',dir1)
  print('dir2=',dir2)
#  print('dir3=',dir3)
#  print('dir4=',dir4)
  files1 = glob.glob(dir1+'*.nc')
  print('files1=',files1) 
  files2 = glob.glob(dir2+'*.nc')
#  files3 = glob.glob(dir3+'*.nc')
#  files4 = glob.glob(dir4+'*.nc')
  #allfiles = set(files1+files2+files3+files4)
  allfiles = set(files1+files2)
  print('allfiles = ', allfiles) 
  for f in allfiles:
    fshort = f.split('/')[-1].split('.')
    print('fshort = ', fshort[0][:])
    print('fshort[0][26:30]=',fshort[0][26:30])
    yyyymmdd=fshort[0][18:26]
    print('yyyymmdd=',yyyymmdd) 
    hhmm=fshort[0][26:30]
    print('hhmm=',hhmm)
    yyyymmddhhmmss=yyyymmdd+hhmm+'00'
    yyyymmddhhmmss=dt.datetime.strptime(yyyymmddhhmmss, '%Y%m%d%H%M%S')
    print('yyyymmddhhmmss=',yyyymmddhhmmss)
    
    fstart = yyyymmddhhmmss #dt.datetime.strptime(yyyymmddhhmmss,"%Y%m%d%H%M%S")
#    print('fstart, StartObs, EndObs',fstart,StartObs,EndObs)
#    print('fstart=',fstart) 
#    fend = dt.datetime.strptime(fshort[4][1:-1],"%Y%m%d%H%M%S")

# Determine which files are within the assimilation window by comparing time/date
    print('StartObs=', StartObs)
    print('fstart=', fstart)
    if (fstart > StartObs) and (fstart < EndObs):
      print('f=',f)
      print('StartObs, fstart, EndObs', StartObs, fstart, EndObs)
      usefiles.append(f)

  validtime=NowCycle.strftime("%Y%m%d%H")
  #print('validtime=',validtime)
  input_flag='-i'
  output_flag='-o'
  OutDir = OutRoot
  output_file = OutDir+'/gdas.t'+str(hour).zfill(2)+'z.viirs_npp.'+validtime+'.nc4'
  method_flag = '-m'
  method = 'nesdis' 
  mask_flag = '-k'
  mask = 'maskout'
  thin_flag = '-n'
  thin_value = str(thinning)  

  usefiles_str = "" 
  for ele in usefiles: 
        usefiles_str += ele+' '  
    

  args=' '+input_flag+' '+usefiles_str+' '+method_flag+' '+method+' '+mask_flag+' '+mask+' '+thin_flag+' '+thin_value+' '+output_flag+' '+output_file
  cmd = executable+args
  print('cmd=',cmd) 
 # with open('test.txt', 'w') as f:
 #     print(cmd,file=f) 
#  print('output_file=',output_file)
  proc = sp.Popen(cmd,env=my_env,shell=True)
  

  if not os.path.exists(OutDir):
    os.makedirs(OutDir)


#  for f in usefiles:
#    fout = OutDir+'/'+f.split('/')[-1]
#    args = ' 'input_flag' 'usefiles' '+f+' '+-o+' '+fout
#    print('args=',args) 

#    cmd = executable+args
#    print('cmd = ', cmd) 

#    proc = sp.Popen(cmd,env=my_env,shell=True)
#    proc.wait() # so that it doesn't overload the system

# concatenate them
#  cmd = 'ncrcat -O '+OutDir+'/*.nc '+OutDir+'/MODIS_C61.'+validtime+'.nc'
#  print('cmd=',cmd ) 
#  proc = sp.Popen(cmd,env=my_env,shell=True)
#  proc.wait()  

#creates larger files with confused dimensions
#  ncfiles = glob.glob(OutDir+'/JRR-AOD_v1r1_npp_*.nc')
##ds = xr.open_mfdataset(outfiles, combine='nested', concat_dim=['nlocs'])
#  ds = xr.open_mfdataset(ncfiles, concat_dim=['nlocs'])
##ds = xr.open_mfdataset(ncfiles)
#  ds.to_netcdf(OutDir+'/viirs_aod_snpp.'+validtime+'-Xarray.nc')

  NowCycle = NowCycle + dt.timedelta(hours=CycleHrs)
