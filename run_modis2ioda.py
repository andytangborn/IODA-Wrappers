#!/usr/bin/env python
# run_viirs2ioda.py
# process VIIRS files and produce JEDI/IODA compatible obs files
import os
import subprocess as sp
import datetime as dt
import glob
#import xarray as xr

#grid='96'
grid='192'

InRoot='/work/noaa/da/Andrew.Tangborn/MODIS'
OutRoot='/work/noaa/da/Andrew.Tangborn/IODA'
#FV3Grid='/scratch1/BMC/gsd-fv3-dev/MAPP_2018/pagowski/fix_fv3/C'+grid
CycleHrs=6

StartCycle=dt.datetime(2016,6,1,6)
EndCycle=dt.datetime(2016,6,1,6)


executable='python /work/noaa/da/Andrew.Tangborn/python/ioda-converters/src/chem/test_getattr.py'
my_env = os.environ.copy()
my_env['OMP_NUM_THREADS'] = '4' # for openmp to speed up fortran call
#./viirs2ioda.x $validtime $fv3dir $infile $outfile

HalfCycle = CycleHrs/2
NowCycle=StartCycle

while NowCycle <= EndCycle:
  print("Processing analysis cycle: "+NowCycle.strftime("%Y-%m-%d_%H:%M UTC"))

  # get +- half of cycle hours
  StartObs = NowCycle - dt.timedelta(hours=HalfCycle)
  EndObs = NowCycle + dt.timedelta(hours=HalfCycle)
  StartObs_doy = StartObs.timetuple().tm_yday
  EndObs_doy = EndObs.timetuple().tm_yday 
  str_start = str(StartObs_doy)
  str_year = str(StartObs.year)
  str_end = str(EndObs_doy)
  
  str_start = 'MOD04_L2.A'+str_year+str_start
  str_end =  'MoD04_L2.A'+str_year+str_end
  # get possible files to use
  usefiles = []
  dir1 = InRoot+'/'+str_start
  dir2 = InRoot+'/'+str_end
  files1 = glob.glob(dir1+'*.hdf')
  files2 = glob.glob(dir2+'*.hdf')
  allfiles = set(files1+files2)
  for f in allfiles:
    fshort = f.split('/')[-1].split('.')
    hhmm=fshort[2][0:4]
    yyyyddd=fshort[1][1:8]
    yyyydddhhmmss=yyyyddd+hhmm+'00'
    yyyymmddhhmmss=dt.datetime.strptime(yyyydddhhmmss, '%Y%j%H%M%S')
    
    
    fstart = yyyymmddhhmmss #dt.datetime.strptime(yyyymmddhhmmss,"%Y%m%d%H%M%S")
#    print('fstart=',fstart) 
#    fend = dt.datetime.strptime(fshort[4][1:-1],"%Y%m%d%H%M%S")

# Determine which files are within the assimilation window by comparing time/date
    if (fstart > StartObs) and (fstart < EndObs):
      usefiles.append(f)

  print('usefiles=',usefiles) 
  validtime=NowCycle.strftime("%Y%m%d%H")
  input_flag='-i'
  output_flag='-o'
  OutDir = OutRoot
  output_file=OutDir+'/MODIS_C61.'+validtime+'.nc'

  usefiles_str = "" 
  for ele in usefiles: 
        usefiles_str += ele+' '  
    

  args=' '+input_flag+' '+usefiles_str+' '+output_flag+' '+output_file
  cmd = executable+args
  print('cmd = ', cmd) 
  proc = sp.Popen(cmd,env=my_env,shell=True)
  

  if not os.path.exists(OutDir):
    os.makedirs(OutDir)

  print(validtime)

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
