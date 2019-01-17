#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 21:47:21 2018

@author: leonidas
"""
#Εξάγει το DNB με τους readers του lliakos

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


debug_on()



# ********* SETTINGS *******************************************
BASEDIR='/media/leonidas/Hitachi/daily_viirs/2017_packed/SDR_DNB'
OUTPUT_DIR_RELATIVE= "geotiffs_201706_QF"  #"LunarZenithAngle" 
PATTERN='GDNBO-SVDNB_npp_d20170310_t2338126_e2343530_b27816_c20181105101747401829_noac_ops.h5'#'GDNBO-SVDNB_npp_d201706*.*h5'
# edge of swath example GDNBO-SVDNB_npp_d20171026_t0124341_e0130145_b31066_c20181106064422380389_nobc_ops.h5


### Greek_Grid area definition
## Greek_Grid area definition
area_id = 'greek_grid'
description = 'Greek Grid'
proj_id = 'greekgrid_2100'
projection = '+proj=tmerc +lat_0=0 +lon_0=24 +x_0=500000 +y_0=0 +ellps=GRS80 +units=m'

#περιοχή νότια της Ύδρας (black object θάλασσα)
#x_size = 40 
#y_size = 40 
#area_extent = (430430, 4087900, 460430, 4117900)

#περιοχή δυτικά της Κεφαλονιάς
#x_size = 100 
#y_size = 100 
#area_extent = (31600, 4218500, 106600, 4293500)


#Αττική
#x_size = 200 
#y_size = 200 
#area_extent = (379354, 4132181, 529354, 4282181)


#Γυάρος (black object ξηρά)
#x_size = 3
#y_size = 4
#area_extent = (562418, 4163046, 564668, 4166046)

#Ελλάδα
x_size = 1080 
y_size = 1050 
area_extent = (101854,3832181,911854,4619681)



area_def = utils.get_area_def(area_id, description, proj_id, projection, x_size, y_size, area_extent)



os.chdir(BASEDIR)
OUTPUT_DIR=os.path.join(BASEDIR,OUTPUT_DIR_RELATIVE )
        
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


swath_files = glob.glob(PATTERN)

txt = open(os.path.join(BASEDIR,"errors.txt"), "a")


for HDF in swath_files:
    try:

        with h5py.File(HDF,'r') as h5_file:
            
            GROUP_DNB_SDR='All_Data/VIIRS-DNB-SDR_All'
            
            radiance        = h5_file[GROUP_DNB_SDR]['Radiance'][...] #h5_file['All_Data/VIIRS-DNB-GEO_All'][DATASET][...]
            qf1_viirsdnbsdr = h5_file[GROUP_DNB_SDR]['QF1_VIIRSDNBSDR'][...]
            qf2_scan_sdr    = h5_file[GROUP_DNB_SDR]['QF2_SCAN_SDR'][...]
            
            
            GROUP_DNB_SDR_GEO='All_Data/VIIRS-DNB-GEO_All'
            
            lon_data             = h5_file[GROUP_DNB_SDR_GEO]['Longitude'][...]
            lat_data             = h5_file[GROUP_DNB_SDR_GEO]['Latitude'][...]
            LunarZenithAngle     = h5_file[GROUP_DNB_SDR_GEO]['LunarZenithAngle'][...]
            qf2_viirssdrgeo      = h5_file[GROUP_DNB_SDR_GEO]['QF2_VIIRSSDRGEO'][...]
            qf2_viirssdrgeo_tc   = h5_file[GROUP_DNB_SDR_GEO]['QF2_VIIRSSDRGEO_TC'][...]
            
        
        #edr= edr.astype('float') 
        #edr[edr==0]=np.nan     
   
        #fill_value=0
        
        #lon_data[radiance == 0] = np.nan
        #lat_data[radiance == 0] = np.nan


        #https://pytroll.slack.com/archives/C06GJFRN0/p1545083373181100
        
        
        
        
        #QF Flags from https://ncc.nesdis.noaa.gov/documents/documentation/viirs-sdr-dataformat.pdf
        
        #1.VIIRS Fill Values (p.70)     
        radiance_fillvalues=np.array([-999.3 ,-999.5, -999.8, -999.9 ])
        radiance_mask = np.isin(radiance, radiance_fillvalues)
        
        #2.Edge-of-swath pixels
        edge_of_swath_mask = np.zeros_like(radiance, dtype='bool')
        edge_of_swath_mask[:,0:254]=1
        edge_of_swath_mask[:,3810:4064]=1
                
        #3.QF1_VIIRSDNBSDR flags (p.73)
        SDR_Quality_mask     = (qf1_viirsdnbsdr&3)>0
        Saturated_Pixel_mask = ((qf1_viirsdnbsdr&12)>>2)>0
        Missing_Data_mask    = ((qf1_viirsdnbsdr&48)>>4)>0
        Out_of_Range_mask    = ((qf1_viirsdnbsdr&64)>>6)>0
                        
        #4.QF2_VIIRSSDRGEO flags (p.90)
        
        qf2_viirssdrgeo_do0_mask= (qf2_viirssdrgeo&1)>0
        qf2_viirssdrgeo_do1_mask= ((qf2_viirssdrgeo&2)>>1)>0
        qf2_viirssdrgeo_do2_mask= ((qf2_viirssdrgeo&4)>>2)>0
        qf2_viirssdrgeo_do3_mask= ((qf2_viirssdrgeo&8)>>3)>0

     
        #Combine pixel level flags
        viirs_mask=np.logical_or.reduce((
                                radiance_mask,
                                edge_of_swath_mask, 
                                SDR_Quality_mask,
                                Saturated_Pixel_mask,
                                Missing_Data_mask,
                                Out_of_Range_mask,
                                qf2_viirssdrgeo_do0_mask,
                                qf2_viirssdrgeo_do1_mask,
                                qf2_viirssdrgeo_do2_mask,
                                qf2_viirssdrgeo_do3_mask, 
                              ))
        
        fill_value=9999.0
        radiance[viirs_mask]=fill_value #set fill value for masked pixels in DNB
        LunarZenithAngle[viirs_mask]=fill_value #set fill value for masked pixels in DNB

        if np.all(radiance == fill_value)==True:
            msg =  "File:{}:{}\n".format(HDF, "Mask applied to all pixels")
            txt.write(msg) 
            continue
            
        
        swath_def = geometry.SwathDefinition(
                xr.DataArray(da.from_array(lon_data, chunks=4096), dims=('y', 'x')), 
                xr.DataArray(da.from_array(lat_data, chunks=4096), dims=('y', 'x')))

        metadata_dict =	{'name': 'dnb', 'area':swath_def}

        scn = Scene()
        scn['Radiance'] = xr.DataArray(
                da.from_array(radiance, chunks=4096), 
                attrs=metadata_dict,
                dims=('y', 'x')) #https://satpy.readthedocs.io/en/latest/dev_guide/xarray_migration.html#id1
        
        scn['LunarZenithAngle'] = xr.DataArray(
                da.from_array(LunarZenithAngle, chunks=4096), 
                attrs=metadata_dict,
                dims=('y', 'x'))    
        
        scn.load(["Radiance"])
        scn.load(["LunarZenithAngle"])
        #print(scn)
        
        proj_scn = scn.resample(area_def)
        
        proj_scn.save_datasets(writer='geotiff',base_dir=OUTPUT_DIR ,file_pattern="{}.{}.{}".format(HDF,"{name}","tif"),enhancement_config=False,
                                       dtype=np.float32,fill_value=fill_value)
        
    except Exception, e:
        msg =  "File:{},Error:{}\n".format(HDF, e)
        txt.write(msg) 


txt.close() 

