from pyresample import utils
import pandas as pd

def getproj(proj_id):
    projections = pd.read_csv("/media/leonidas/Hitachi/daily_viirs/2017_packed/batchHDF2Geotiff/projections.csv",sep = ';')
    myproj=projections.loc[projections['proj_id'] == proj_id]
    
    proj_id     = myproj.iloc[0]['proj_id']  
    description = myproj.iloc[0]['description']  
    projection  = myproj.iloc[0]['projection']      

    return proj_id, description, projection
    



#### AREA SETTINGS
def getarea(area_id):
    areas = pd.read_csv("/media/leonidas/Hitachi/daily_viirs/2017_packed/batchHDF2Geotiff/areas.csv",sep = ';')
    myarea=areas.loc[areas['area_id'] == area_id]
    
    area_id = myarea.iloc[0]['area_id'] 
    x_size = myarea.iloc[0]['x_size'] 
    y_size = myarea.iloc[0]['y_size']  
    proj_id = myarea.iloc[0]['proj_id']  
    
    area_extent = (myarea.iloc[0]['xmin'] ,myarea.iloc[0]['ymin'] ,myarea.iloc[0]['xmax'] ,myarea.iloc[0]['ymax'] )
    
    ##Grid area 
    proj_id,description, projection = getproj(proj_id)
    
    area_def = utils.get_area_def(area_id, description, proj_id, projection, x_size, y_size, area_extent)
    return area_def
