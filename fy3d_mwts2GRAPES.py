#!/usr/bin/env python
# coding=UTF-8
'''
@Author: wanghao
@Date: 2019-09-28 10:02:10
@LastEditors: Hejun Xie
@LastEditTime: 2020-05-21 19:04:00
@Description: 
'''
import numpy as np
import h5py
import datetime
import os
import struct 
from tkinter import _flatten
import time

class type_rad1c(object):
    def __init__(self):
        self.yyyy, self.mn, self.dd, self.hh, self.mn, self.ss = 0, 0, 0, 0, 0, 0
        self.iscanline, self.iscanpos       = 0, 0
        self.rlat, self.rlon                = 0., 0.
        self.isurf_height, self.isurf_type  = 0, 0
        self.satzen, self.satazi, self.solzen, self.solazi = 0., 0., 0., 0.
        self.tbb    = np.zeros(13, dtype=">f4")
        self.iavhrr = np.zeros(13, dtype=">i4")
        self.ihirsflag  = 0
        self.iprepro    = np.zeros(5, dtype=">i4")
        self.clfra      = 0.
        self.ts         = 0.
        self.tctop      = 0.

# define constant
ddate     = "2019082212" #yyyymnddhh
channels  = 13
scanpos   = 90
MDI       = -999999
RMDI      = -999999.

w0step = 24  # hours, Start day and End day increment 
w1step = 6   # hours, Time window length, hours
w2step = 3   # hours, 

dw0step = datetime.timedelta(hours=w0step)
dw1step = datetime.timedelta(hours=w1step)
dw2step = datetime.timedelta(hours=w2step)

cstart_time = datetime.datetime.strptime(ddate,"%Y%m%d%H")

# generate filenames
c0tart_time, c0end_time  = cstart_time-dw0step, cstart_time+dw0step
r0start_time, r0end_time = datetime.datetime.strftime(c0tart_time,"%Y%m%d"), datetime.datetime.strftime(c0end_time,"%Y%m%d")  # Time window begin and end

filenames = []
for idate in [r0start_time, ddate[0:8], r0end_time]:
    filenames_tmp = os.listdir("./OBSDATA/{}".format(idate))
    for ifilename in filenames_tmp:
        bool = ifilename.endswith(".HDF")
        if bool:
            filenames.append("{}".format(ifilename))

# extract needed files
c1tart_time, c1end_time  = cstart_time-dw1step, cstart_time+dw1step
r1start_time, r1end_time = datetime.datetime.strftime(c1tart_time,"%Y%m%d%H"), datetime.datetime.strftime(c1end_time,"%Y%m%d%H")  # Time window begin and end

use_filenames = []
for ifile in filenames:
    time_tmp = ifile[19:27]+ifile[28:30]
    if r1start_time < time_tmp < r1end_time:
        use_filenames.append("./OBSDATA/{}/{}".format(ifile[19:27],ifile))

print("use files:")
start = time.time()
for inn,ifile in enumerate(use_filenames):
    print(ifile)
    fili = h5py.File(ifile,"r")
    if inn == 0:
        # Geolocation数据集
        vs_Geolocation   = fili["Geolocation"]
        vs_Latitude      = vs_Geolocation["Latitude"][:].flatten()
        vs_Longitude     = vs_Geolocation["Longitude"][:].flatten()
        # area data
        vs_DEM           = vs_Geolocation["DEM"][:].flatten()
        vs_LandSeaMask   = vs_Geolocation["LandSeaMask"][:].flatten()
        # angle
        vs_SensorZenith  = vs_Geolocation["SensorZenith"][:].flatten()
        vs_SensorAzimuth = vs_Geolocation["SensorAzimuth"][:].flatten()
        vs_SolarZenith   = vs_Geolocation["SolarZenith"][:].flatten()
        vs_SolarAzimuth  = vs_Geolocation["SolarAzimuth"][:].flatten()

        # Data数据集
        vs_Data        = fili["Data"]
        vs_Tbb         = np.reshape(vs_Data["Earth_Obs_BT"][:], (channels, -1))
        scanlines      = fili.attrs["Number Of Scans"][0]
        vs_Obs_time    = vs_Data["Time"][:]

        vs_times_str_tmp = np.array(["{}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(vs_Obs_time[i][0],vs_Obs_time[i][1],
                                                                             vs_Obs_time[i][2],vs_Obs_time[i][3],vs_Obs_time[i][4]) for i in np.arange(scanlines)])
        vs_times_str = []
        for itime in vs_times_str_tmp:
            for i in np.arange(scanpos):
                vs_times_str.append(itime)
    else:
        # Geolocation数据集
        v_Geolocation   = fili["Geolocation"]
        v_Latitude      = v_Geolocation["Latitude"][:].flatten()
        v_Longitude     = v_Geolocation["Longitude"][:].flatten()
        # area data
        v_DEM           = v_Geolocation["DEM"][:].flatten()
        v_LandSeaMask   = v_Geolocation["LandSeaMask"][:].flatten()
        # angle
        v_SensorZenith  = v_Geolocation["SensorZenith"][:].flatten()
        v_SensorAzimuth = v_Geolocation["SensorAzimuth"][:].flatten()
        v_SolarZenith   = v_Geolocation["SolarZenith"][:].flatten()
        v_SolarAzimuth  = v_Geolocation["SolarAzimuth"][:].flatten()

        # Data数据集
        v_Data        = fili["Data"]
        v_Tbb         = np.reshape(v_Data["Earth_Obs_BT"][:], (channels,-1))
        scanlines     = fili.attrs["Number Of Scans"][0]
        v_Obs_time    = v_Data["Time"][:]
        v_times_str_tmp = np.array(["{}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(v_Obs_time[i][0],v_Obs_time[i][1],
                                                                            v_Obs_time[i][2],v_Obs_time[i][3],v_Obs_time[i][4]) for i in np.arange(scanlines)])
        v_times_str = []
        for itime in v_times_str_tmp:
            for i in np.arange(scanpos):
                v_times_str.append(itime)

        # conecet data
        vs_Latitude      = np.concatenate([vs_Latitude,       v_Latitude])
        vs_Longitude     = np.concatenate([vs_Longitude,      v_Longitude])
        vs_DEM           = np.concatenate([vs_DEM,            v_DEM])
        vs_LandSeaMask   = np.concatenate([vs_LandSeaMask,    v_LandSeaMask])
        vs_SensorZenith  = np.concatenate([vs_SensorZenith,   v_SensorZenith])
        vs_SensorAzimuth = np.concatenate([vs_SensorAzimuth,  v_SensorAzimuth])
        vs_SolarZenith   = np.concatenate([vs_SolarZenith,    v_SolarZenith])
        vs_SolarAzimuth  = np.concatenate([vs_SolarAzimuth,   v_SolarAzimuth])
        vs_Tbb           = np.concatenate([vs_Tbb,            v_Tbb], axis=1)
        vs_times_str     = np.concatenate([vs_times_str,      v_times_str])

# 
c2start_time, c2end_time  = cstart_time-dw2step, cstart_time+dw2step
r2start_time, r2end_time = datetime.datetime.strftime(c2start_time,"%Y%m%d%H"), datetime.datetime.strftime(c2end_time,"%Y%m%d%H")  # Time window begin and end

indexes = np.argwhere((vs_times_str > r2start_time) & (vs_times_str < r2end_time)).flatten()
nobs    = len(indexes)

uTime_str      = vs_times_str[indexes]
uDEM           = vs_DEM[indexes]
uLandSeaMask   = vs_LandSeaMask[indexes]
uSensorZenith  = vs_SensorZenith[indexes]
uSensorAzimuth = vs_SensorAzimuth[indexes]
uSolarZenith   = vs_SolarZenith[indexes]
uSolarAzimuth  = vs_SolarAzimuth[indexes]
ulat           = vs_Latitude[indexes]
ulon           = vs_Longitude[indexes]
utbb           = vs_Tbb[:,indexes]

# 定义结构体数组
arr = []
for i in np.arange(nobs):
    arr.append(type_rad1c())

# print(uTime_str)
for iobs in np.arange(nobs):
    arr[iobs].yyyy         = int(uTime_str[iobs][0:4])
    arr[iobs].mn           = int(uTime_str[iobs][4:6])
    arr[iobs].dd           = int(uTime_str[iobs][6:8])
    arr[iobs].hh           = int(uTime_str[iobs][8:10])
    arr[iobs].mn           = int(uTime_str[iobs][10:12])
    arr[iobs].ss           = int(00)
    arr[iobs].iscanline    = (iobs+1)//scanpos
    arr[iobs].iscanpos     = np.mod(iobs+1, scanpos)
    arr[iobs].rlat         = ulat[iobs]
    arr[iobs].rlon         = ulon[iobs]
    arr[iobs].isurf_height = uDEM[iobs]
    arr[iobs].isurf_type   = uLandSeaMask[iobs]
    arr[iobs].satzen       = uSensorZenith[iobs]
    arr[iobs].satazi       = uSensorAzimuth[iobs]
    arr[iobs].solzen       = uSolarZenith[iobs]
    arr[iobs].solazi       = uSolarAzimuth[iobs]
    arr[iobs].tbb          = utbb[:,iobs]*0.01
    arr[iobs].iavhrr[:]    = MDI
    arr[iobs].ihirsflag    = MDI
    arr[iobs].iprepro[:]   = 0
    arr[iobs].clfra        = RMDI
    arr[iobs].ts           = RMDI
    arr[iobs].tctop        = RMDI
end = time.time()
print("process data use %d seconds"%(end-start))

def _pack_structure(brr):
    """ Pack a structure from a dictionary """
    fmt = ">iiiiiiiiiffiifffffffffffffffffiiiiiiiiiiiiiiiiiiifffi"
    values_tmp =   [204,brr.yyyy,brr.mn,brr.dd,brr.hh,brr.mn,brr.ss,
                        brr.iscanline, brr.iscanpos,
                        brr.rlat, brr.rlon,
                        brr.isurf_height, brr.isurf_type,
                        brr.satzen, brr.satazi, brr.solzen, brr.solazi,
                        brr.tbb[:].tolist(),
                        brr.iavhrr[:].tolist(),
                        brr.ihirsflag,
                        brr.iprepro[:].tolist(),
                        brr.clfra,
                        brr.ts,
                        brr.tctop,204]
    vals = _flatten(values_tmp)
    return struct.pack(fmt, *vals)

# output file
start = time.time()
with open ("fy3d_ama{}00".format(ddate),"wb") as fhandle:
    print("begin to write data")
    for iobs in np.arange(nobs):
        fhandle.write(_pack_structure(arr[iobs]))
fhandle.close()
end = time.time()
print("output file use %s seconds"%(end-start))
