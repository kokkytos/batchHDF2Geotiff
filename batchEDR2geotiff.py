#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 22:58:41 2018

@author: leonidas
"""

#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 18:13:37 2018

@author: leonidas
"""
# 
# In[7]:https://pytroll.slack.com/archives/C06GJFRN0/p1545083373181100


import h5py
import numpy as np
from pyresample import geometry
import os,glob
import xarray as xr
import dask.array as da
from satpy import  Scene 
from pyresample import utils
from satpy.utils import debug_on
import numpy.ma as ma
debug_on()


# ********* SETTINGS *******************************************
BASEDIR='/media/leonidas/Hitachi/daily_viirs/2017_packed/EDR_CLOUD_MASK'
OUTPUT_DIR_RELATIVE="geotiffs_bitoperations"
PATTERN='GMODO-VICMO_npp_d20170313_t0039560_e0045364_b27845_c20181105104612702584_noac_ops.h5'#'GMODO-VICMO_npp_d201703*.h5'
DATASET = 'QF1_VIIRSCMEDR'

## Greek_Grid area definition
area_id = 'greek_grid'
description = 'Greek Grid'
proj_id = 'greekgrid_2100'
projection = '+proj=tmerc +lat_0=0 +lon_0=24 +x_0=500000 +y_0=0 +ellps=GRS80 +units=m'

#περιοχή νότια της Ύδρας (black object)
#x_size = 40 
#y_size = 40 
#area_extent = (430430, 4087900, 460430, 4117900)

#περιοχή δυτικά της Κεφαλονιάς
#x_size = 100 
#y_size = 100 
#area_extent = (31600, 4218500, 106600, 4293500)


#Αττική
x_size = 200 
y_size = 200 
area_extent = (379354, 4132181, 529354, 4282181)

#Γυάρος (black object ξηρά)
#x_size = 3
#y_size = 4
#area_extent = (562418, 4163046, 564668, 4166046)

area_def = utils.get_area_def(area_id, description, proj_id, projection, x_size, y_size, area_extent)


os.chdir(BASEDIR)
OUTPUT_DIR=os.path.join(BASEDIR,OUTPUT_DIR_RELATIVE)

        
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


swath_files = glob.glob(PATTERN)

txt = open(os.path.join(BASEDIR,"errors.txt"), "a")


for HDF in swath_files:
    try:

        with h5py.File(HDF,'r') as h5_file:
            edr=h5_file['All_Data/VIIRS-CM-EDR_All'][DATASET][...]
            lon_data=h5_file['All_Data/VIIRS-MOD-GEO_All']['Longitude'][...]
            lat_data=h5_file['All_Data/VIIRS-MOD-GEO_All']['Latitude'][...]
        
        fill_value=-1
        
        
        lon_data[edr == 0] = np.nan
        lat_data[edr == 0] = np.nan
        
        #apply bit operations
        edr=(edr&12)>>2
        #edr=edr.astype('float')#cannot set np.nan to integer data type,only to float
        #edr[edr==0]=np.nan     
        
        #export clouds DN=1, noclouds=0
        m_edr = ma.masked_greater(edr, 0)
        m_edr = ma.filled(m_edr, fill_value=1)

        #https://pytroll.slack.com/archives/C06GJFRN0/p1545083373181100
        
        swath_def = geometry.SwathDefinition(
                xr.DataArray(da.from_array(lon_data, chunks=4096), dims=('y', 'x')), 
                xr.DataArray(da.from_array(lat_data, chunks=4096), dims=('y', 'x')))

        metadata_dict =	{'name': 'cloud_mask', 'area':swath_def}

        scn = Scene()
        scn['cloud_mask'] = xr.DataArray(
                da.from_array(m_edr, chunks=4096), 
                attrs=metadata_dict,
                dims=('y', 'x')) #https://satpy.readthedocs.io/en/latest/dev_guide/xarray_migration.html#id1
        
        scn.load(["cloud_mask"])
        print(scn)
        
        proj_scn = scn.resample(area_def)
        
        proj_scn.save_datasets(writer='geotiff',base_dir=OUTPUT_DIR ,file_pattern="{}.{}.{}".format(HDF,DATASET,"tif"),enhancement_config=False,
                                       dtype=np.float32)
        
    except Exception, e:
        msg =  "File:{},Error:{}\n".format(file, e)
        txt.write(msg) 


txt.close() 

