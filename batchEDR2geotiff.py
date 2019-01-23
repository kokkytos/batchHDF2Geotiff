#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 22:58:41 2018

@author: leonidas
"""

import h5py
import numpy as np
from pyresample import geometry
import os,glob
import xarray as xr
import dask.array as da
from satpy import  Scene 
from satpy.utils import debug_on
import areaSettings
import argparse
import ntpath


def maskByte(byte):
    # Joint Polar Satellite System (JPSS)-Algorithm Specification Volume II: Data-Dictionary for the Cloud Mask:
    # https://jointmission.gsfc.nasa.gov/sciencedocs/2016-12/474-00448-02-11_JPSS-DD-Vol-II-Part-11_0200E.pdf
    
    # Cloud Detection and Confidence Pixel
    CloudDetection_ConfidencePixel_mask=(byte & 12)>>2 #apply bitwise operations
    
    # export clouds DN=1, noclouds=0
    # Confidently Clear = 0
    # Probably Clear = 1
    # Probably Cloudy = 2
    # Confidently Cloudy = 3
    # keep only Confidently Clear
    # CloudDetection_ConfidencePixel_mask= ma.masked_greater(CloudDetection_ConfidencePixel_mask, 0)
    # CloudDetection_ConfidencePixel_mask = ma.filled(CloudDetection_ConfidencePixel_mask, fill_value=1)
    
    # Snow/Ice Surface Pixel
    # No Snow/Ice 0
    #Snow/Ice 1

    SnowIce_mask=(byte & 32)>>5 #apply bitwise operations
    
    #    HeavyAerosol_mask=(QF2_VIIRSCMEDR & 16)>>4
    #    Cirrus_mask=(QF2_VIIRSCMEDR & 64)>>6
    #    CirrusIR_mask=(QF2_VIIRSCMEDR & 128)>>7
    #           
    #    Dust_Candidate_mask=(QF4_VIIRSCMEDR & 16)>>4
    #    Smoke_Candidate_mask=(QF4_VIIRSCMEDR & 32)>>5
    #    Dust_VolcanicAsh_mask=(QF4_VIIRSCMEDR & 64)>>6

    
    
    #Combine pixel level flags
    mask=np.logical_or.reduce((
            CloudDetection_ConfidencePixel_mask,
            SnowIce_mask,
#                HeavyAerosol_mask,
#                Cirrus_mask,
#                CirrusIR_mask,
#                Dust_Candidate_mask,
#                Smoke_Candidate_mask,
#                Dust_VolcanicAsh_mask,
                          ))
    return mask


def readhdfDatasets(HDF):
    with h5py.File(HDF,'r') as h5_file:
            GROUP_DATA='All_Data/VIIRS-CM-EDR_All'
            QF1_VIIRSCMEDR=h5_file[GROUP_DATA]['QF1_VIIRSCMEDR'][...]
            #QF2_VIIRSCMEDR=h5_file['All_Data/VIIRS-CM-EDR_All']['QF2_VIIRSCMEDR'][...]
            #QF4_VIIRSCMEDR=h5_file['All_Data/VIIRS-CM-EDR_All']['QF4_VIIRSCMEDR'][...]
            GROUP_GEODATA='All_Data/VIIRS-MOD-GEO_All'
            lon_data=h5_file[GROUP_GEODATA]['Longitude'][...]
            lat_data=h5_file[GROUP_GEODATA]['Latitude'][...]   
            
    return QF1_VIIRSCMEDR, lon_data,lat_data


def EDR2Geotiff(HDF, output_dir,areaid, radius):
    

    QF1_VIIRSCMEDR, lon_data, lat_data = readhdfDatasets(HDF)
    HDF = ntpath.basename(HDF)#get filename without path
        
    #https://pytroll.slack.com/archives/C06GJFRN0/p1545083373181100            
    lon_data[QF1_VIIRSCMEDR == 0] = np.nan
    lat_data[QF1_VIIRSCMEDR == 0] = np.nan
    
    mask=maskByte(byte=QF1_VIIRSCMEDR)
    mask=mask.astype(np.uint8)
    
    fill_value=255 #fill_value is parameter of save_datasets(...). satpy sets 255 for pixels not included in my AOI.
            
    
    swath_def = geometry.SwathDefinition(
            xr.DataArray(da.from_array(lon_data, chunks=4096), dims=('y', 'x')), 
            xr.DataArray(da.from_array(lat_data, chunks=4096), dims=('y', 'x')))

    metadata_dict =	{'name': 'mask', 'area':swath_def}

    scn = Scene()
    scn['mask'] = xr.DataArray(
            da.from_array(mask, chunks=4096), 
            attrs=metadata_dict,
            dims=('y', 'x')) #https://satpy.readthedocs.io/en/latest/dev_guide/xarray_migration.html#id1
    
    scn.load(["mask"])
   
    proj_scn = scn.resample(areaSettings.getarea(areaid),radius_of_influence=radius)
    
    proj_scn.save_datasets(writer='geotiff',base_dir=output_dir ,file_pattern="{}.{}.{}".format(HDF,"{name}","tif"),enhancement_config=False,
                                   dtype=np.uint8, fill_value=fill_value) # 
        

def batchEDR2Geotiff(input_dir, output_dir,pattern, areaid, debug):
    # ********* SETTINGS *******************************************
    #input_dir='/media/leonidas/Hitachi/daily_viirs/2017_packed/EDR_CLOUD_MASK'
    #output_dir="geotiffs_greece2"
   # pattern='GMODO-VICMO_npp_d20170331_t2346150_e2351554_b28114_c20181105104544883294_noac_ops.h5'
    #'GMODO-VICMO_npp_d201703*.h5'
    #'GMODO-VICMO_npp_d201703*.h5'
    #'GMODO-VICMO_npp_d20170328_t0056143_e0101547_b28058_c20181105104553745389_noac_ops.h5'#GMODO-VICMO_npp_d20170328_t0056143_e0101547_b28058_c20181105104553745389_noac_ops.h5'##'GMODO-VICMO_npp_d201703*.h5'#'GMODO-VICMO_npp_d20170328_t0056143_e0101547_b28058_c20181105104553745389_noac_ops.h5'#GMODO-VICMO_npp_d20170313_t0039560_e0045364_b27845_c20181105104612702584_noac_ops.h5
    
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
        EDR2Geotiff(hdf, output_dir, areaid,radius=2000)
        
def dir_path(string):
    string = os.path.abspath(string) 
    if os.path.isdir(string):
        return string
    else:
        print('Provide a valid directory.')
        raise NotADirectoryError(string)

def parse_args():
    parser = argparse.ArgumentParser(description='Extracts Cloud Mask from VIIRS EDR hdf file (Specific Quality flags) as binary geotiff.0=Confidently Clear')
    parser.add_argument("-i", "--inputdir", help="Directory contains hdf5 files",    required=True, type=dir_path)
    parser.add_argument("-o", "--outputdir",   help="Directory to export geotiff files",required=True, type=dir_path)
    parser.add_argument("-p", "--pattern",  help="Regex pattern to match hdf files", default='GMODO-VICMO_npp_d*.h5')
    parser.add_argument("-a", "--areaid",  help="Area_ID",required=True)
    parser.add_argument("-d", "--debug", action='store_true')

    args = parser.parse_args()
    
    return args

def main():
    args = parse_args()
    batchEDR2Geotiff(input_dir=args.inputdir, output_dir=args.outputdir, pattern=args.pattern, areaid=args.areaid, debug=args.debug)

if __name__ == '__main__':
    main()
