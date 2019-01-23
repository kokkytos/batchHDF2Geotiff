#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 21:47:21 2018

@author: leonidas
"""


import h5py
import numpy as np
from pyresample import geometry
import os,glob
import xarray as xr
import dask.array as da
from satpy import  Scene 
import areaSettings
from satpy.utils import debug_on

debug_on()

# ********* SETTINGS *******************************************
BASEDIR='/media/leonidas/Hitachi/daily_viirs/2017_packed/SDR_DNB'
OUTPUT_DIR_RELATIVE= "geotiffs_greece"  #"LunarZenithAngle" 
PATTERN='GDNBO-SVDNB_npp_d20170328_t0056143_e0101547_b28058_c20181105101755851757_noac_ops.h5'
#'GDNBO-SVDNB_npp_d20170328_t0056143_e0101547_b28058_c20181105101755851757_noac_ops.h5'
#'GDNBO-SVDNB_npp_d20170310_t2338126_e2343530_b27816_c20181105101747401829_noac_ops.h5'
#'GDNBO-SVDNB_npp_d201706*.*h5'
# edge of swath example GDNBO-SVDNB_npp_d20171026_t0124341_e0130145_b31066_c20181106064422380389_nobc_ops.h5


os.chdir(BASEDIR)
OUTPUT_DIR=os.path.join(BASEDIR,OUTPUT_DIR_RELATIVE )


        
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


swath_files = glob.glob(PATTERN)


for HDF in swath_files:

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
        SDR_Quality_mask     = (qf1_viirsdnbsdr & 3)>0
        Saturated_Pixel_mask = ((qf1_viirsdnbsdr & 12)>>2)>0
        Missing_Data_mask    = ((qf1_viirsdnbsdr & 48)>>4)>0
        Out_of_Range_mask    = ((qf1_viirsdnbsdr & 64)>>6)>0
                        
        #4.QF2_VIIRSSDRGEO flags (p.90)
        
        qf2_viirssdrgeo_do0_mask= (qf2_viirssdrgeo & 1)>0
        qf2_viirssdrgeo_do1_mask= ((qf2_viirssdrgeo & 2)>>1)>0
        qf2_viirssdrgeo_do2_mask= ((qf2_viirssdrgeo & 4)>>2)>0
        qf2_viirssdrgeo_do3_mask= ((qf2_viirssdrgeo & 8)>>3)>0

     
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
        
        fill_value=255.0
        radiance[viirs_mask]=fill_value #set fill value for masked pixels in DNB
        LunarZenithAngle[viirs_mask]=fill_value #set fill value for masked pixels in DNB

        
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
        
        proj_scn = scn.resample(areaSettings.area_def,radius_of_influence=2000)
        
        proj_scn.save_datasets(writer='geotiff',base_dir=OUTPUT_DIR ,file_pattern="{}.{}.{}".format(HDF,"{name}","tif"),enhancement_config=False,
                                       dtype=np.float32,fill_value=fill_value)
        

