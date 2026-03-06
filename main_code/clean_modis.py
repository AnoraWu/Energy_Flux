import numpy as np
import xarray as xr
import os
import sys
import gc
import glob
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import multiprocessing as mp
import geopandas as gpd
from pyhdf.SD import SD, SDC
from shapely.geometry import Point
from multiprocessing import Pool
from scipy.interpolate import griddata
from pyproj import Transformer
from datetime import datetime, timedelta
from shapely.geometry import mapping
from matplotlib.path import Path

warnings.filterwarnings(
    "ignore",
    message="Duplicate dimension names present: dimensions {'vecdim'}",
    category=UserWarning,
    module="xarray.namedarray.core"
)

check = "True"
data_dir = "/Users/anora/Team MG Dropbox/Wanru Wu/Energy_Flux"

# Keep only pixels in produced grids
grid = gpd.read_file(f"{data_dir}/intermediate/jx_grid.gpkg", layer='grid').dissolve() 
geom = grid.geometry.iloc[0]

# Fast bbox filter
minx, miny, maxx, maxy = geom.bounds

# From Modis documentation: The MODIS cloud product is a L2 product, and is archived in version 4 of a self-described
# Hierarchical Data Format (HDF4) file based upon the platform (Terra or Aqua) and temporal
# period of collection (every 5 minutes along the orbit track). One 5 min file, or data granule,
# contains data from roughly 2330 km across-track (1354 1 km pixels) to 2000 km along-track
# of Earth located data. 
# So we open with the pyhdf module


# Helper function: clean variables
def get_real_value(hdf_original, geo=False):
    '''
    This function calculate and return the real value of the input dataset.
    The real value, according to documentation, is 
    calculated as scale_factor * (integer_value – add_offset)
    '''
    hdf = hdf_original[:]
    # convert to float to assign NA values
    # will not change number signs or values
    if hdf.dtype in ['float32','int8','int16']:
        hdf = hdf.astype('float32') 
        hdf[hdf == hdf_original.attributes()['_FillValue']] = np.nan
        if geo == False:
            hdf = (hdf - hdf_original.attributes()['add_offset']) * hdf_original.attributes()['scale_factor']
        return hdf.flatten()
    else:
        print("Error: this data type should not use this function.")
        sys.exit()

# Helper function: convert lon and lat to "EPSG:32650", to convert degrees to meters
transformer = Transformer.from_crs("EPSG:4326", "EPSG:32650", always_xy=True)


year = 2020
cloud_file = "MOD06_L2"
data_1km_collector = []
data_5km_collector = []

for day in range(1,8):
    print(f"{year} {day}")
    cloud_paths = sorted(glob.glob(f"{data_dir}/modis_l2/{cloud_file}/{year}/{day}/{cloud_file}.A{year}{str(day).zfill(3)}.*.hdf"))
    print("Found", len(cloud_paths), "files")

    for cp in cloud_paths: 
        # Read MOD06 data

        # Get time variable
        # According to documentation, the time is UTC time. 
        # To align with operation data, we convert to Beijing (UTC+8) time.
        start = cp.find("MOD06_L2.")
        ac_year = int(cp[start+10:start+14])
        ac_day = int(cp[start+14:start+17])
        ac_hour = int(cp[start+18:start+20])
        ac_min = int(cp[start+20:start+22])

        utc_time = datetime(ac_year, 1, 1) + timedelta(days=ac_day - 1, hours=ac_hour, minutes=ac_min)
        beijing_time = utc_time + timedelta(hours=8)

        # Construct corresponding path for MOD03 geolocation file
        cp_geo_list = glob.glob(cp.replace("MOD06_L2","MOD03")[:-17] + f"*.hdf")
        if len(cp_geo_list) == 1:
            cp_geo = cp_geo_list[0]
        else:
            print(f"Error: MOD03 file for file {cp} doesn't exist.")
            sys.exit()
            
        hdf = SD(cp, SDC.READ)

        # Firstly, process 5km variables
        # Get lat, lon, 5km cloud variables
        lat = get_real_value(hdf.select('Latitude'))
        lon = get_real_value(hdf.select('Longitude'))

        # Convert to "EPSG:32650"
        xs, ys = transformer.transform(lon, lat)
        bbox_mask_5km= (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
        points = np.column_stack([xs[bbox_mask_5km], ys[bbox_mask_5km]])

        # Precise containment
        coords = np.array(mapping(geom)['coordinates'][0])
        inside = Path(coords).contains_points(points)

        # Create mask, keep only points within Jiangxi grids
        mask_5km= bbox_mask_5km.copy()
        mask_5km[bbox_mask_5km] = inside

        # If mask_5km.sum() == 0, it means that this swath is not covered in Jiangxi grids
        # Then 1km pixels is not contained as well, as 5km pixels is aggregated from 1km pixels
        if mask_5km.sum() == 0:
            hdf.end()
            continue

        xs = xs[mask_5km]
        ys = ys[mask_5km]
        cloud_fraction = get_real_value(hdf.select('Cloud_Fraction'))[mask_5km]
        cloud_fraction_night = get_real_value(hdf.select('Cloud_Fraction_Night'))[mask_5km]
        cloud_fraction_day = get_real_value(hdf.select('Cloud_Fraction_Day'))[mask_5km]

        # Apply quality masks
        # If _FillValue == 0, we don't need to convert to NA,
        # as 00000000 indicates invalid values, which will be filtered out
        qa5_a = hdf.select('Quality_Assurance_5km').attributes()

        # For quality_assurance, we want to preserve the int8 data type,
        # so we don't do the math or insert NA values which requires float data type
        if (qa5_a["_FillValue"] == 0) & (qa5_a['add_offset'] == 0) & (qa5_a['scale_factor'] == 1):

            # Convert to data array. will not change data type
            qa5_hdf = hdf.select('Quality_Assurance_5km')[:]

            # According to documentation, we need tackle negative signs. 
            # The method described in the document is equivalent to conert int8 (signed int) to uint8 (unsigned int)
            qa5_hdf = qa5_hdf.astype("uint8")

            # Data is very large (60T in total), so it's hard to change quality assurance masks once data is processed
            # To facilitate later changes, we add quality masks as a variable, makeing it easy
            # to apply different masks later
            qa5_dict = {}
            for i in range(0, 10):
                qa5_dict[f"qa5_byte_{i}"] = qa5_hdf[:,:,i].flatten()[mask_5km]

        else:
            print("FillValue != 0 or add_offset != 0 or scale_factor!= 1. Error." )
            sys.exit()

        # Construct data frame and apply quality masks to data frame later
        data_5km = pd.DataFrame(np.column_stack([xs, ys, cloud_fraction, cloud_fraction_night, cloud_fraction_day] + list(qa5_dict.values())))
        data_5km.columns=['lon','lat','cloud_fraction','cloud_fraction_night','cloud_fraction_day'] + list(qa5_dict.keys())
        data_5km['date_time'] = beijing_time
        data_5km_collector.append(data_5km)

        # Clean memory
        del cloud_fraction, cloud_fraction_night, cloud_fraction_day
        del lat, lon, xs, ys
        del qa5_dict, qa5_hdf, data_5km
        _ = gc.collect()


        # Secondly, process 1km variables

        # Read geolocation files
        hdf_geo = SD(cp_geo, SDC.READ)
        lat = get_real_value(hdf_geo.select('Latitude'), geo = True)
        lon = get_real_value(hdf_geo.select('Longitude'), geo = True)

        # Convert to "EPSG:32650"
        xs, ys = transformer.transform(lon, lat)
        bbox_mask_1km= (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
        points = np.column_stack([xs[bbox_mask_1km], ys[bbox_mask_1km]])

        # Precise containment
        coords = np.array(mapping(geom)['coordinates'][0])
        inside = Path(coords).contains_points(points)

        # Create mask, keep only points within Jiangxi grids
        mask_1km= bbox_mask_1km.copy()
        mask_1km[bbox_mask_1km] = inside

        xs = xs[mask_1km]
        ys = ys[mask_1km]
        cloud_optical_thickness = get_real_value(hdf.select('Cloud_Optical_Thickness'))[mask_1km]
        cloud_optical_thickness_uncertainty = get_real_value(hdf.select('Cloud_Optical_Thickness_Uncertainty'))[mask_1km]

        # Apply quality masks
        # If _FillValue == 0, we don't need to convert to NA,
        # as 00000000 indicates invalid values, which will be filtered out
        qa1_a = hdf.select('Quality_Assurance_1km').attributes()

        # For quality_assurance, we want to preserve the int8 data type,
        # so we don't do the math or insert NA values which requires float data type
        if (qa1_a["_FillValue"] == 0) & (qa1_a['add_offset'] == 0) & (qa1_a['scale_factor'] == 1):

            # Convert to data array. will not change data type
            qa1_hdf = hdf.select('Quality_Assurance_1km')[:]

            # According to documentation, we need tackle negative signs. 
            # The method described in the document is equivalent to conert int8 (signed int) to uint8 (unsigned int)
            qa1_hdf = qa1_hdf.astype("uint8")

            # Data is very large (60T in total), so it's hard to change quality assurance masks once data is processed
            # To facilitate later changes, we add quality masks as a variable, makeing it easy
            # to apply different masks later
            qa1_dict = {}
            for i in range(0, 9):
                qa1_dict[f"qa1_byte_{i}"] = qa1_hdf[:,:,i].flatten()[mask_1km] 

        else:
            print("FillValue != 0 or add_offset != 0 or scale_factor!= 1. Error." )
            sys.exit()

        hdf.end()
        hdf_geo.end()

        # Construct data frame and apply quality masks to data frame later
        data_1km = pd.DataFrame(np.column_stack([xs, ys, cloud_optical_thickness, cloud_optical_thickness_uncertainty] + list(qa1_dict.values())))
        data_1km.columns=['lon','lat','cloud_optical_thickness','cloud_optical_thickness_uncertainty'] + list(qa1_dict.keys())
        data_1km['date_time'] = beijing_time
        data_1km_collector.append(data_1km)

        # Clean memory
        del cloud_optical_thickness, cloud_optical_thickness_uncertainty
        del lat, lon, xs, ys
        del qa1_dict, qa1_hdf, data_1km
        _ = gc.collect()

