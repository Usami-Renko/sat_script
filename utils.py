'''
@Description: some utilities
@Author: Hejun Xie
@Date: 2020-05-21 23:02:37
@LastEditors: Hejun Xie
@LastEditTime: 2020-05-21 23:03:23
'''

import os
import sys
import yaml


def makenewdir(mydir):
    if not os.path.exists(mydir):
        os.system("mkdir {}".format(mydir))
        os.system("chmod -R o-w {}".format(mydir))

def config(config_path, config_file):
    cong_yamlPath = os.path.join(config_path, config_file)
    if sys.version_info[0] < 3:
        cong = yaml.load(open(cong_yamlPath))
    elif sys.version_info[0] >= 3:
        try:
            cong = yaml.load(open(cong_yamlPath), Loader=yaml.FullLoader)
        except:
            cong = yaml.load(open(cong_yamlPath))
    return cong

def config_list(config_path, config_files):
    cong = dict()
    for config_file in config_files:
        new_cong = config(config_path, config_file)
        cong.update(new_cong)
    
    return cong

