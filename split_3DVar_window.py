#!/usr/bin/env python
# coding=UTF-8

'''
@Description: split satellite data from old 3D var windows into new 3D windows
@Author: Hejun Xie
@Date: 2020-05-21 22:50:57
@LastEditors: Hejun Xie
@LastEditTime: 2020-05-22 12:54:01
'''

# global import
import os
import sys
import struct
import glob
import struct
import numpy as np
import datetime as dt


# local import
from utils import config_list, makenewdir

CONFIGPATH = './config/' # default config path
cong = config_list(CONFIGPATH, ['config.yml'])

# config script
for key, value in cong.items():
    globals()[key] = value

class SatWorkStation(object):
    def __init__(self, sat_name, old_dir, new_dir, \
                 old_window, new_window, \
                 start_dt, end_dt, nchannels):
        self.sat_name = sat_name
        self.old_dir = old_dir
        self.new_dir = new_dir
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
        cursor_olddt = self.start_dt
        cursor_newdt = self.start_dt
        
        # Iterate over newdts
        while(True):
            if cursor_newdt > self.end_dt:
                break

            # generate new window
            cursor_newfile = self._generate_file_dt(cursor_newdt, self.new_dir)
            cursor_newwindow = Window(cursor_newdt, self.new_window, cursor_newfile, self.recordLen, self.fmt)
            
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

class WindowAssemble(object):
    def __init__(self):
        self.start_dt = None
        self.end_dt = None

        # The window list is maintained as a queue
        self.windowList = []

    def enqueueWindow(self, window):
        '''
        Enqueue a new window and update the coverage of WindowAssemble
        '''
        
        if self.start_dt == None:
            self.start_dt = window.start_dt
        self.end_dt = window.end_dt
        
        self.windowList.append(window)

        # print('Extend window to: {} {}'.format(self.start_dt, self.end_dt))

    def dequeueWindow(self):
        '''
        Dequeue an old window it is no longer needed
        '''
        self.windowList[0].close()
        del(self.windowList[0])
        
        if len(self.windowList) == 0:
            self.start_dt = None
            self.end_dt = None
        else:
            self.start_dt = self.windowList[0].start_dt
        
        # print('Cut window to: {} {}'.format(self.start_dt, self.end_dt))
    
    def parse_data(self, newwindow):
        '''
        Iterate over all the windows in the windowList and put data into the new window  
        '''

        for oldwindow in self.windowList:
            newwindow.filter_data(oldwindow)
        
        newwindow.write_data()
    
    def close(self):
        for window in self.windowList:
            window.close()


class Window(object):
    def __init__(self, center_dt, window_len, filename, recordLen, fmt):
        self.center_dt = center_dt
        self.window_len = window_len
        self.filename = filename
        self.recordLen = recordLen
        self.fmt = fmt
        self.recordData = []
        self.recordTime = []

        self.start_dt = self.center_dt - dt.timedelta(hours=self.window_len) / 2.
        self.end_dt = self.center_dt + dt.timedelta(hours=self.window_len) / 2.

        # print('Create window: {} {}'.format(self.start_dt, self.end_dt))

    def containedBy(self, another_window):
        if another_window.start_dt == None or another_window.end_dt == None:
            return False

        if self.start_dt >= another_window.start_dt and \
            self.end_dt <= another_window.end_dt:
            return True
        else:
            return False
    
    def intersectWith(self, another_window):
        if another_window.start_dt == None or another_window.end_dt == None:
            return False

        if another_window.start_dt <= self.end_dt <= another_window.end_dt or \
           another_window.start_dt <= self.start_dt <= another_window.end_dt or \
           self.start_dt <= another_window.start_dt <= self.end_dt or \
           self.start_dt <= another_window.end_dt <= self.end_dt:
            return True
        else:
            return False
    
    def include(self, dt):
        if dt >= self.start_dt and dt <= self.end_dt:
            return True
        else:
            return False

    def close(self):
        del self.recordData
        del self.recordTime

    def load_data(self):
        if not os.path.exists(self.filename):
            raise IOError("Satellite data {} not found !".format(self.filename))
        
        with open(self.filename,"rb") as fhandle:
            while(True):
                Data = fhandle.read(self.recordLen)
                if not Data:
                    break
                FData = struct.unpack(self.fmt, Data)
                year, month, day, hour, minute, second = FData[1], FData[2], FData[3], FData[4], FData[5], FData[6]
                Time = dt.datetime(year, month, day, hour, minute, second)
                self.recordData.append(Data)
                self.recordTime.append(Time)

            
    def write_data(self):
        with open(self.filename,'wb') as fhandle:
            for Data in self.recordData:
                fhandle.write(Data)
        self.close()
    
    def filter_data(self, another_window):
        if self.intersectWith(another_window):
           for Data, Time in zip(another_window.recordData, another_window.recordTime):
               if self.include(Time):
                   self.recordData.append(Data)
                   self.recordTime.append(Time) 


if __name__ == "__main__":

    start_dt    = dt.datetime.strptime(start_str, "%Y%m%d%H%M")
    end_dt      = dt.datetime.strptime(end_str, "%Y%m%d%H%M")

    makenewdir(newwindow_dir)

    satellites = nchannels.keys()

    for satellite in satellites:
        print(satellite)
        sws = SatWorkStation(satellite, oldwindow_dir, newwindow_dir, \
                             oldwindow_len, newwindow_len, \
                             start_dt, end_dt, nchannels[satellite])
        
        sws.split_data()
        sws.close()
