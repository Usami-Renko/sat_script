#!/usr/bin/env python
# coding=UTF-8

'''
@Description: split satellite data from old 3D var windows into new 3D windows
@Author: Hejun Xie
@Date: 2020-05-21 22:50:57
@LastEditors: Hejun Xie
@LastEditTime: 2020-05-22 15:33:22
'''

# global import
import os
import sys
import glob
import numpy as np
import datetime as dt


# local import
from utils import makenewdir
from window import WindowAssemble, Window

class SatWorkStation(object):
    def __init__(self, sat_name, old_dir, new_dir, \
                 old_window, new_window, old_start_dt, \
                 start_dt, end_dt, nchannels):
        self.sat_name = sat_name
        self.old_dir = old_dir
        self.new_dir = new_dir
        self.old_start_dt = old_start_dt
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.nchannels = nchannels
        self.old_window = old_window
        self.new_window = new_window

        self.fmt = None
        self.recordLen = None
        self._get_recordLen()

        self.filelist = []
        self.get_file_list()

        self.aliveWindows = WindowAssemble()
    
    def get_file_list(self):
        allfiles = glob.glob("{}/{}_*.dat".format(self.old_dir, self.sat_name))

        for allfile in allfiles:
            file_dt = self._get_file_dt(allfile)
            if self.start_dt <= file_dt <= self.end_dt:
                self.filelist.append(allfile)
    
    def _get_recordLen(self):
        '''
        Assign the binary record format and record length
        '''
        self.fmt = ">iiiiiiiiiffiiffff"+ "f"*self.nchannels + "i"*13 + 'i' + 'i'*5 + "fffi"
        self.recordLen = (len(self.fmt) - 1) * 4

    def _get_file_dt(self, filename):
        if self.sat_name in ['metop1', 'metop2', 'noaa15', 'noaa18', 'noaa19']:
            time_stamp = filename.split('/')[-1].split('.')[0].split('_')[1]
        elif self.sat_name in ['npp']:
            time_stamp = filename.split('/')[-1].split('.')[0].split('_')[1].strip('atms')
        else:
            raise ValueError('Unknown sattelite name {}'.format(self.sat_name))
        
        return dt.datetime.strptime(time_stamp, '%Y%m%d%H')
    
    def _generate_file_dt(self, dt, filedir):
        time_stamp = dt.strftime('%Y%m%d%H')
        if self.sat_name in ['metop1', 'metop2', 'noaa15', 'noaa18', 'noaa19']:
            return "{}/{}_{}_ama.dat".format(filedir, self.sat_name, time_stamp)
        elif self.sat_name in ['npp']:
            return "{}/{}_atms{}.dat".format(filedir, self.sat_name, time_stamp)
        else:
            raise ValueError('Unknown sattelite name {}'.format(self.sat_name))

    def split_data(self):
        cursor_olddt = self.old_start_dt
        cursor_newdt = self.start_dt
        
        # Iterate over newdts
        while(True):
            if cursor_newdt > self.end_dt:
                break

            # generate new window
            cursor_newfile = self._generate_file_dt(cursor_newdt, self.new_dir)
            cursor_newwindow = Window(cursor_newdt, self.new_window, cursor_newfile, self.recordLen, self.fmt)
            
            # check if the new window can be generated
            if cursor_newwindow.start_dt <= self.start_dt - dt.timedelta(hours=self.old_window)/2.:
                cursor_newdt += dt.timedelta(hours=self.new_window)
                continue
            
            if cursor_newwindow.end_dt >= self.end_dt + dt.timedelta(hours=self.old_window)/2.:
                break
            
            # Copnfirmed: can be generated
            print('New Sat File: {}'.format(cursor_newfile))

            # Iterate over old window list and dequeue the old window that do not intersect with new window 
            while(True):
                # print('Alive window length: {}'.format(len(self.aliveWindows.windowList)))
                if len(self.aliveWindows.windowList) == 0:
                    break

                if not cursor_newwindow.intersectWith(self.aliveWindows.windowList[0]):
                    self.aliveWindows.dequeueWindow()
                else:
                    break

            # Iterate over olddts and enqueue them until the window assemble covers new window
            while(True):
                if cursor_newwindow.containedBy(self.aliveWindows):
                    break

                # generate old window and load data
                # print('Old')
                cursor_oldfile = self._generate_file_dt(cursor_olddt, self.old_dir)
                cursor_oldwindow = Window(cursor_olddt, self.old_window, cursor_oldfile, self.recordLen, self.fmt)
                cursor_oldwindow.load_data()
    
                self.aliveWindows.enqueueWindow(cursor_oldwindow)
                cursor_olddt += dt.timedelta(hours=self.old_window)
                

            # split data and write new data
            
            self.aliveWindows.parse_data(cursor_newwindow)
            cursor_newdt += dt.timedelta(hours=self.new_window)

        # exit()

    def close(self):
        self.aliveWindows.close()
        del self.filelist




if __name__ == "__main__":

    # configuration
    oldwindow_dir = sys.argv[1]
    newwindow_dir = sys.argv[2]
    start_str = sys.argv[3]

    # date
    end_str     =  start_str
    
    # channels
    nchannels = {
    "metop1": 20,
    "metop2": 20,
    "noaa15": 20,
    "noaa18": 20,
    "noaa19": 20,
    "npp": 22
    }

    # window length [hours]
    oldwindow_len = 6 
    newwindow_len = 3

    # new dt
    start_dt        = dt.datetime.strptime(start_str, "%Y%m%d%H")
    end_dt          = dt.datetime.strptime(end_str, "%Y%m%d%H")
    
    # old dt
    if start_dt.hour % oldwindow_len == 0:
        old_start_dt = start_dt
    else:
        old_start_dt = start_dt - dt.timedelta(hours=start_dt.hour%oldwindow_len)

    makenewdir(newwindow_dir)

    satellites = nchannels.keys()

    for satellite in satellites:
        print(satellite)
        sws = SatWorkStation(satellite, oldwindow_dir, newwindow_dir, \
                             oldwindow_len, newwindow_len, old_start_dt, \
                             start_dt, end_dt, nchannels[satellite])
        
        sws.split_data()
        sws.close()
