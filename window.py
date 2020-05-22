'''
@Description: window class
@Author: Hejun Xie
@Date: 2020-05-22 12:54:35
@LastEditors: Hejun Xie
@LastEditTime: 2020-05-22 12:57:17
'''
import os
import sys
import struct
import numpy as np
import datetime as dt


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
