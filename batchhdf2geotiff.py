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
import argparse
import ntpath


def readhdfDatasets(HDF):
    with h5py.File(HDF,'r') as h5_file:
            GROUP_DNB_SDR='All_Data/VIIRS-DNB-SDR_All'
            
            radiance        = h5_file[GROUP_DNB_SDR]['Radiance'][...] 
            qf1_viirsdnbsdr = h5_file[GROUP_DNB_SDR]['QF1_VIIRSDNBSDR'][...]
            qf2_scan_sdr    = h5_file[GROUP_DNB_SDR]['QF2_SCAN_SDR'][...]
            
            
            GROUP_DNB_SDR_GEO='All_Data/VIIRS-DNB-GEO_All'
            
            lon_data             = h5_file[GROUP_DNB_SDR_GEO]['Longitude'][...]
            lat_data             = h5_file[GROUP_DNB_SDR_GEO]['Latitude'][...]
            LunarZenithAngle     = h5_file[GROUP_DNB_SDR_GEO]['LunarZenithAngle'][...]
            qf2_viirssdrgeo      = h5_file[GROUP_DNB_SDR_GEO]['QF2_VIIRSSDRGEO'][...]
            qf2_viirssdrgeo_tc   = h5_file[GROUP_DNB_SDR_GEO]['QF2_VIIRSSDRGEO_TC'][...]
            
    return radiance, qf1_viirsdnbsdr, qf2_scan_sdr, lon_data, lat_data, LunarZenithAngle, qf2_viirssdrgeo, qf2_viirssdrgeo_tc


def SDR2Geotiff(HDF, output_dir,areaid, radius):
    radiance,qf1_viirsdnbsdr,qf2_scan_sdr,lon_data,lat_data,LunarZenithAngle,qf2_viirssdrgeo,qf2_viirssdrgeo_tc = readhdfDatasets(HDF)
    HDF = ntpath.basename(HDF)#get filename without path
    
    #QF Flags from https://ncc.nesdis.noaa.gov/documents/documentation/viirs-sdr-dataformat.pdf
    
    #1.VIIRS Fill Values (p.70)     
    radiance_fillvalues=np.array([-999.3 ,-999.5, -999.8, -999.9 ])
    radiance_mask = np.isin(radiance, radiance_fillvalues)
    
    #2.Edge-of-swath pixels
    edge_of_swath_mask = np.zeros_like(radiance, dtype='bool')
    edge_of_swath_mask[:,0:254]=1
    edge_of_swath_mask[:,3810:4064]=1
            
    #3.QF1_VIIRSDNBSDR flags (p.73)
    #SDR_Quality_mask     = (qf1_viirsdnbsdr & 3)>0
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
                            #SDR_Quality_mask,
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
    
    proj_scn = scn.resample(areaSettings.getarea(areaid),radius_of_influence=radius)
    
    proj_scn.save_datasets(writer='geotiff',base_dir=output_dir ,file_pattern="{}.{}.{}".format(HDF,"{name}","tif"),enhancement_config=False,
                                   dtype=np.float32,fill_value=fill_value)
       





def batchSDR2Geotiff(input_dir, output_dir,pattern, areaid, debug):
    if debug:
        debug_on()
    curdir=os.getcwd()
    os.chdir(input_dir)
    #OUTPUT_DIR=os.path.join(input_dir,OUTPUT_DIR_RELATIVE)
    output_dir = os.path.abspath(output_dir) #works for abs and relative paths
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    hdf_files = glob.glob(os.path.join(input_dir,pattern))
    os.chdir(curdir)
    for hdf in hdf_files:
        SDR2Geotiff(hdf, output_dir, areaid,radius=2000)        

def dir_path(string):
    string = os.path.abspath(string) 
    if os.path.isdir(string):
        return string
    else:
        print('Provide a valid directory.')
        raise NotADirectoryError(string)

def parse_args():
    parser = argparse.ArgumentParser(description='Converts VIIRS SDR DNB hdf dataset to geotiff. Applies Mask for Specific Quality flags.')
    parser.add_argument("-i", "--inputdir", help="Directory that contains hdf5 files",    required=True, type=dir_path)
    parser.add_argument("-o", "--outputdir",   help="Directory to export geotiff files",required=True, type=dir_path)
    parser.add_argument("-p", "--pattern",  help="Regex pattern to match hdf files", default='GMODO-VICMO_npp_d*.h5')
    parser.add_argument("-a", "--areaid",  help="Area_ID",required=True)
    parser.add_argument("-d", "--debug", action='store_true')

    args = parser.parse_args()
    
    return args

def main():
    args = parse_args()
    batchSDR2Geotiff(input_dir=args.inputdir, output_dir=args.outputdir, pattern=args.pattern, areaid=args.areaid, debug=args.debug)

if __name__ == '__main__':
    main()
    
    
#runfile('/media/leonidas/Hitachi/daily_viirs/2017_packed/batchHDF2Geotiff/batchhdf2geotiff.py', wdir='/media/leonidas/Hitachi/daily_viirs/2017_packed/batchHDF2Geotiff' ,args = '-i /media/leonidas/Hitachi/daily_viirs/2017_packed/SDR_DNB -o /media/leonidas/Hitachi/daily_viirs/2017_packed/SDR_DNB/attiki_cmd_062017 -p GDNBO-SVDNB_npp_d201706*.*h5 -a greek_grid4')