# -*- coding: utf-8 -*-
#Εξάγει το DNB με τους readers του satpy


from satpy import find_files_and_readers, Scene
from datetime import datetime
from pyresample import geometry
from satpy.utils import debug_on
import numpy as np
import glob,os
from pyresample import utils

#question at seec forum:https://forums.ssec.wisc.edu/viewtopic.php?f=39&t=49323&p=52828#p52828

debug_on()


### ETRS89/LAEA area definition
area_id = 'etrs89-etrs-laea'
description = 'ETRS89 / ETRS-LAEA'
proj_id = 'etrs89_3035'
x_size = 5915 #3867
y_size = 3764 #3918
#area_extent = (2426378.0132, 1528101.2618, 6293974.6215, 5446513.5222)
area_extent = (2426378, 1286990, 6862628, 4109990)
proj_dict = {'units': 'm',
             'proj': 'laea',
             'lat_0': 52,
             'lon_0': 10,
             'x_0' : 4321000,
             'y_0' : 3210000,
             'ellps':'GRS80'
             }


### Greek_Grid area definition
area_id = 'greek_grid'
description = 'Greek Grid'
proj_id = 'greekgrid_2100'
x_size = 100 
y_size = 100 
area_extent = (31600, 4218500, 106600, 4293500)
projection = '+proj=tmerc +lat_0=0 +lon_0=24 +x_0=500000 +y_0=0 +ellps=GRS80 +units=m'

#area_def = geometry.AreaDefinition(area_id, description, proj_id, proj_dict, x_size, y_size, area_extent) #area_extent: (x_ll, y_ll, x_ur, y_ur)

area_def = utils.get_area_def(area_id, description, proj_id, projection, x_size, y_size, area_extent)
#print(area_def)


BASEDIR='/media/leonidas/Hitachi/daily_viirs/2017/3332700175/001'
OUTPUT_DIR=os.path.join(BASEDIR, "geotiffs")


os.chdir(BASEDIR)
swath_files = glob.glob('SVDNB_npp_d*.h5')

txt = open(os.path.join(BASEDIR,"errors.txt"), "a")



if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    

for file in swath_files:
    try:

        
        scene = Scene(filenames=[file], reader='viirs_sdr')
        scene.load(["DNB"]) #alternative method: scene.load([0.7]). You ca also load other composites like "dynamic_dnb", adaptive_dnb,histogram_dnb ,hncc_dnb. Check scene.available_composite_names()
        print(scene)
        
        proj_scn = scene.resample(area_def)
        
        proj_scn.save_datasets(writer='geotiff',
                               base_dir=OUTPUT_DIR,
                               file_pattern='{name}_{start_time:%Y%m%d_%H%M%S}_{end_time:%Y%m%d_%H%M%S}_so{start_orbit}_eo{end_orbit}_epsg2100.tif',
                               enhancement_config=False,
                               dtype=np.float32)
    except Exception, e:
        msg =  "File:{},Error:{}\n".format(file, e)
        txt.write(msg) 


txt.close() 
