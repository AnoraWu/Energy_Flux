import numpy as np
import os
import sys
import gc
import glob
import warnings
import geopandas as gpd
from pyhdf.SD import SD, SDC
from multiprocessing import Pool
from pyproj import Transformer
from shapely.geometry import mapping
from matplotlib.path import Path

warnings.filterwarnings("ignore")

data_dir = "Volumes/project/mgreenst/energy_flux"

# Keep only pixels in produced grids
grid = gpd.read_file(f"{data_dir}/intermediate/jx_grid.gpkg", layer='grid').dissolve()
geom = grid.geometry.iloc[0]
coords = np.array(mapping(geom)['coordinates'][0])
poly_path = Path(coords)

# Fast bbox filter
minx, miny, maxx, maxy = geom.bounds


def process_file(cp):
    try:
        # Each worker needs its own transformer
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:32650", always_xy=True)

        # Construct corresponding path for MOD03 geolocation file
        cp_geo_list = glob.glob(cp.replace("MOD06_L2", "MOD03")[:-17] + "*.hdf")
        if len(cp_geo_list) != 1:
            return None

        cp_geo = cp_geo_list[0]
        hdf = SD(cp, SDC.READ)
        hdf_geo = SD(cp_geo, SDC.READ)

        # Read geolocation
        lat_sds = hdf_geo.select('Latitude')
        lon_sds = hdf_geo.select('Longitude')
        lat = lat_sds[:].astype('float32').flatten()
        lon = lon_sds[:].astype('float32').flatten()
        lat_fill = lat_sds.attributes()['_FillValue']
        lon_fill = lon_sds.attributes()['_FillValue']

        # Mark fill values as NaN
        lat[lat == lat_fill] = np.nan
        lon[lon == lon_fill] = np.nan

        # Convert to EPSG:32650
        xs, ys = transformer.transform(lon, lat)
        bbox_mask = np.isfinite(xs) & np.isfinite(ys) & (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)

        if bbox_mask.sum() == 0:
            hdf.end()
            hdf_geo.end()
            return None

        # Precise containment
        points = np.column_stack([xs[bbox_mask], ys[bbox_mask]])
        inside = poly_path.contains_points(points)
        mask_1km = bbox_mask.copy()
        mask_1km[bbox_mask] = inside

        if mask_1km.sum() == 0:
            hdf.end()
            hdf_geo.end()
            return None

        # Read COT
        cot_sds = hdf.select('Cloud_Optical_Thickness')
        cot = cot_sds[:].astype('float32').flatten()[mask_1km]
        cot_fill = cot_sds.attributes()['_FillValue']

        # Read Cloud Top Temperature 1km
        ctt_sds = hdf.select('cloud_top_temperature_1km')
        ctt = ctt_sds[:].astype('float32').flatten()[mask_1km]
        ctt_fill = ctt_sds.attributes()['_FillValue']

        # Read Cloud Top Pressure 1km
        ctp_sds = hdf.select('cloud_top_pressure_1km')
        ctp = ctp_sds[:].astype('float32').flatten()[mask_1km]
        ctp_fill = ctp_sds.attributes()['_FillValue']

        hdf.end()
        hdf_geo.end()

        # Count
        valid_cot = cot != cot_fill
        n_valid = int(valid_cot.sum())
        n_either = int((valid_cot & ((ctt == ctt_fill) | (ctp == ctp_fill))).sum())

        return (n_valid, n_either)

    except Exception as e:
        print(f"Error: {cp}: {e}", flush=True)
        return None


if __name__ == "__main__":

    year = 2024
    total_valid_cot = 0
    total_either_fill = 0
    files_checked = 0

    for day in range(1, 101):
        cloud_paths = sorted(glob.glob(
            f"{data_dir}/modis_l2/MOD06_L2/{year}/{str(day).zfill(3)}/MOD06_L2.A{year}{str(day).zfill(3)}.*.hdf"
        ))

        if len(cloud_paths) == 0:
            continue

        with Pool(processes=10) as pool:
            results = pool.map(process_file, cloud_paths)

        for r in results:
            if r is not None:
                total_valid_cot += r[0]
                total_either_fill += r[1]
                files_checked += 1

        print(f"Day {day:3d} | files: {files_checked} | "
              f"valid COT: {total_valid_cot:,} | either fill: {total_either_fill:,}", flush=True)

        del results
        gc.collect()

    print("\n" + "=" * 60)
    print(f"Files checked:            {files_checked:,}")
    print(f"Pixels with valid COT:    {total_valid_cot:,}")
    print(f"  COT valid, either fill: {total_either_fill:,}  "
          f"({100 * total_either_fill / max(total_valid_cot, 1):.3f}%)")
    print("=" * 60)
