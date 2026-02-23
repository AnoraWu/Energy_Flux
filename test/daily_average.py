import sys
print("PYTHON:", sys.executable)

import numpy as np
import xarray as xr
import os
import glob
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import multiprocessing as mp
from multiprocessing import Pool
from scipy.interpolate import griddata
from pyproj import Transformer

warnings.filterwarnings(
    "ignore",
    message="Duplicate dimension names present: dimensions {'vecdim'}",
    category=UserWarning,
    module="xarray.namedarray.core"
)


# PLOTTING

def plot_5km_grid(mean_grid, x_bins, y_bins, title=""):
    plt.figure(figsize=(10, 10))
    cmap = plt.get_cmap("viridis").with_extremes(bad="lightgray")

    plt.imshow(
        mean_grid,
        origin="lower",
        cmap=cmap,
        extent=[x_bins[0], x_bins[-1], y_bins[0], y_bins[-1]]
    )
    plt.colorbar(label="Cloud Fraction")
    plt.title(title)
    plt.xlabel("x (meters in EPSG:4527)")
    plt.ylabel("y (meters in EPSG:4527)")
    plt.show()



# 5KM WORKER FUNCTION

def daily_grid_jx_5km(year, day, jx_wkt_proj, jx_bounds, cloud_file, var, grid_size=5000):

    print(f"[5km] {year} {day} {var}", flush=True)

    # Import heavy geospatial libs INSIDE worker
    import geopandas as gpd
    from shapely import wkt
    from shapely.geometry import Point

    # Rebuild projected polygon
    jx_proj = wkt.loads(jx_wkt_proj)
    minx, miny, maxx, maxy = jx_bounds

    # Build 5km grid
    x_bins = np.arange(minx, maxx + grid_size, grid_size)
    y_bins = np.arange(miny, maxy + grid_size, grid_size)

    sum_grid = np.zeros((len(y_bins)-1, len(x_bins)-1))
    count_grid = np.zeros((len(y_bins)-1, len(x_bins)-1))

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:4527", always_xy=True)

    cloud_paths = sorted(glob.glob(f"{cloud_file}/{year}/{day}/{cloud_file}.A{year}{day}.*.hdf"))
    print("Found", len(cloud_paths), "files", flush=True)

    if len(cloud_paths) == 0:
        empty = np.full((len(y_bins)-1, len(x_bins)-1), np.nan)
        return year, day, var, empty, empty, x_bins, y_bins

    # Process each swath
    for cp in cloud_paths:
        try:
            cloud = xr.open_dataset(cp, engine="netcdf4", decode_times=False, mask_and_scale=True)

            lat = cloud["Latitude"].data
            lon = cloud["Longitude"].data
            dat = cloud[var].data

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                qa = cloud["Quality_Assurance_5km"].astype("uint16")

            cot_use = (qa[:, :, 1] >> 0) & 1
            cot_mask = (cot_use == 1)

            if cot_mask.sum().item() == 0:
                cloud.close()
                continue

            mask = np.isfinite(lat) & np.isfinite(lon) & np.isfinite(dat) & cot_mask
            lat = lat[mask]
            lon = lon[mask]
            dat = dat[mask]

            xs, ys = transformer.transform(lon, lat)

            inbbox = (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
            if np.sum(inbbox) == 0:
                cloud.close()
                continue

            xs = xs[inbbox]
            ys = ys[inbbox]
            dat = dat[inbbox]

            # Filter polygon
            pts = gpd.GeoSeries(gpd.points_from_xy(xs, ys), crs="EPSG:4527")
            inside = pts.within(jx_proj).values
            if inside.sum() == 0:
                cloud.close()
                continue

            xs = xs[inside]
            ys = ys[inside]
            dat = dat[inside]

            H_sum = np.histogram2d(ys, xs, bins=[y_bins, x_bins], weights=dat)[0]
            H_cnt = np.histogram2d(ys, xs, bins=[y_bins, x_bins])[0]

            sum_grid += H_sum
            count_grid += H_cnt

            cloud.close()

        except Exception as e:
            print("Error:", cp, e, flush=True)
            continue

    # Compute mean grid
    count2 = count_grid.copy()
    count2[count2 == 0] = 1
    mean_grid = sum_grid / count2
    mean_grid[count_grid == 0] = np.nan

    # Interpolate missing
    x_centers = (x_bins[:-1] + x_bins[1:]) / 2
    y_centers = (y_bins[:-1] + y_bins[1:]) / 2
    yy, xx = np.meshgrid(y_centers, x_centers, indexing='ij')

    nan_mask = np.isnan(mean_grid)
    known_points = np.stack((xx[~nan_mask], yy[~nan_mask]), axis=-1)
    known_values = mean_grid[~nan_mask]

    if known_points.shape[0] < 4:
        blank = np.full(mean_grid.shape, np.nan)
        return year, day, var, blank, mean_grid, x_bins, y_bins

    target_points = np.stack((xx.ravel(), yy.ravel()), axis=-1)
    interp_vals = griddata(known_points, known_values, target_points, method="linear")
    filled_grid = interp_vals.reshape(mean_grid.shape)

    # Mask outside polygon
    from shapely.geometry import Point
    inside_mask_flat = np.array([Point(x, y).within(jx_proj) for x, y in target_points])
    inside_mask = inside_mask_flat.reshape(mean_grid.shape)

    final = mean_grid.copy()
    final[nan_mask & inside_mask] = filled_grid[nan_mask & inside_mask]
    final[~inside_mask] = np.nan

    return year, day, var, final, mean_grid, x_bins, y_bins



# 1KM WORKER

def daily_grid_jx_1km(year, day, jx_wkt_proj, jx_bounds, cloud_file, var, grid_size=5000):

    print(f"[1km] {year} {day} {var}", flush=True)

    import geopandas as gpd
    from shapely import wkt
    from shapely.geometry import Point

    jx_proj = wkt.loads(jx_wkt_proj)
    minx, miny, maxx, maxy = jx_bounds

    x_bins = np.arange(minx, maxx + grid_size, grid_size)
    y_bins = np.arange(miny, maxy + grid_size, grid_size)

    sum_grid = np.zeros((len(y_bins)-1, len(x_bins)-1))
    count_grid = np.zeros((len(y_bins)-1, len(x_bins)-1))

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:4527", always_xy=True)

    cloud_paths = sorted(glob.glob(f"{cloud_file}/{year}/{day}/{cloud_file}.A{year}{day}.*.hdf"))
    geo_paths   = sorted(glob.glob(f"MOD03/{year}/{day}/MOD03.A{year}{day}.*.hdf"))

    print("Found", len(cloud_paths), "cloud files,", len(geo_paths), "geo files", flush=True)

    if len(cloud_paths) == 0 or len(geo_paths) == 0:
        empty = np.full((len(y_bins)-1, len(x_bins)-1), np.nan)
        return year, day, var, empty, empty, x_bins, y_bins

    # Map timestamp → geo file
    geo_by_time = {}
    for g in geo_paths:
        ts = os.path.basename(g).split('.')[2]
        geo_by_time[ts] = g

    # Process cloud files
    for cp in cloud_paths:
        try:
            fname = os.path.basename(cp)
            ts = fname.split('.')[2]

            if ts not in geo_by_time:
                continue

            cloud = xr.open_dataset(cp, engine="netcdf4", decode_times=False, mask_and_scale=True)
            geo   = xr.open_dataset(geo_by_time[ts], engine="netcdf4", decode_times=False, mask_and_scale=True)

            lat = geo["Latitude"].data
            lon = geo["Longitude"].data
            dat = cloud[var].data

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                qa = cloud["Quality_Assurance_1km"].astype("uint16")

            cot_use = (qa[:, :, 0] >> 0) & 1
            cot_mask = (cot_use == 1)
            if cot_mask.sum().item() == 0:
                cloud.close(); geo.close()
                continue

            # phase
            phase_arr = qa[:, :, 2]
            phase_val = (phase_arr >> 0) & ((1 << 3) - 1)
            phase_mask = (phase_val != 0)
            if phase_mask.sum().item() == 0:
                cloud.close(); geo.close()
                continue

            # retrieval
            ret_val = (phase_arr >> 3) & ((1 << 1) - 1)
            ret_mask = (ret_val != 0)
            if ret_mask.sum().item() == 0:
                cloud.close(); geo.close()
                continue

            mask = np.isfinite(lat) & np.isfinite(lon) & np.isfinite(dat) & cot_mask & phase_mask & ret_mask
            lat = lat[mask]
            lon = lon[mask]
            dat = dat[mask]

            xs, ys = transformer.transform(lon, lat)

            inbbox = (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
            if np.sum(inbbox) == 0:
                cloud.close(); geo.close()
                continue

            xs = xs[inbbox]
            ys = ys[inbbox]
            dat = dat[inbbox]

            # Filter polygon
            pts = gpd.GeoSeries(gpd.points_from_xy(xs, ys), crs="EPSG:4527")
            inside = pts.within(jx_proj).values
            if inside.sum() == 0:
                cloud.close(); geo.close()
                continue

            xs = xs[inside]
            ys = ys[inside]
            dat = dat[inside]

            # grid
            H_sum = np.histogram2d(ys, xs, bins=[y_bins, x_bins], weights=dat)[0]
            H_cnt = np.histogram2d(ys, xs, bins=[y_bins, x_bins])[0]

            sum_grid += H_sum
            count_grid += H_cnt

            cloud.close()
            geo.close()

        except Exception as e:
            print("Error:", cp, e, flush=True)
            continue

    # mean
    count2 = count_grid.copy()
    count2[count2 == 0] = 1
    mean_grid = sum_grid / count2
    mean_grid[count_grid == 0] = np.nan

    # interpolation
    x_centers = (x_bins[:-1] + x_bins[1:]) / 2
    y_centers = (y_bins[:-1] + y_bins[1:]) / 2
    yy, xx = np.meshgrid(y_centers, x_centers, indexing='ij')

    nan_mask = np.isnan(mean_grid)
    known_points = np.stack((xx[~nan_mask], yy[~nan_mask]), axis=-1)
    known_values = mean_grid[~nan_mask]

    if known_points.shape[0] < 4:
        blank = np.full(mean_grid.shape, np.nan)
        return year, day, var, blank, mean_grid, x_bins, y_bins

    target_points = np.stack((xx.ravel(), yy.ravel()), axis=-1)
    interp_vals = griddata(known_points, known_values, target_points, method="linear")
    filled = interp_vals.reshape(mean_grid.shape)

    # mask outside polygon
    from shapely.geometry import Point
    inside_mask_flat = np.array([Point(x, y).within(jx_proj) for x, y in target_points])
    inside_mask = inside_mask_flat.reshape(mean_grid.shape)

    final = mean_grid.copy()
    final[nan_mask & inside_mask] = filled[nan_mask & inside_mask]
    final[~inside_mask] = np.nan

    return year, day, var, final, mean_grid, x_bins, y_bins



# MAIN


if __name__ == "__main__":

    mp.set_start_method("spawn", force=True)

    import geopandas as gpd

    print("Loading shapefile...", flush=True)

    os.chdir("/project/mgreenst/cloudseeding/rawdata")

    # Load and project JX polygon ONCE
    jx_poly = gpd.read_file('jiangxi/jiangxi_shape.shp').geometry.iloc[0]
    jx_poly_proj = gpd.GeoSeries([jx_poly], crs="EPSG:4326").to_crs("EPSG:4527").iloc[0]

    jx_wkt_proj = jx_poly_proj.wkt               # safe for spawn
    jx_bounds = jx_poly_proj.bounds              # simple tuple → safe

    print("Shapefile loaded & projected", flush=True)

    km5vars = ["Cloud_Fraction", "Cloud_Fraction_Night", "Cloud_Fraction_Day"]
    km1vars = ["Cloud_Optical_Thickness", "Cloud_Optical_Thickness_Uncertainty"]

    years = ["2020"]
    days = [f"{d:03d}" for d in range(1, 367)]

    km5_tuple = [(y, d, jx_wkt_proj, jx_bounds, "MOD06_L2", var)
                 for y in years for d in days for var in km5vars]

    km1_tuple = [(y, d, jx_wkt_proj, jx_bounds, "MOD06_L2", var)
                 for y in years for d in days for var in km1vars]

    print("Task tuples constructed", flush=True)

    n = int(os.environ.get("SLURM_CPUS_PER_TASK", 15))

    results = []

    print("Running 5km tasks...", flush=True)
    with Pool(n) as p:
        results_5km = p.starmap(daily_grid_jx_5km, km5_tuple)
    results.append(results_5km)

    print("Running 1km tasks...", flush=True)
    with Pool(n) as p:
        results_1km = p.starmap(daily_grid_jx_1km, km1_tuple)
    results.append(results_1km)

    print("Constructing DataFrame...", flush=True)

    # Convert output to panel
    records = []

    for result_list in results:
        for result in result_list:
            year, day, var, final_grid, raw_grid, x_bins, y_bins = result

            x_centers = (x_bins[:-1] + x_bins[1:]) / 2
            y_centers = (y_bins[:-1] + y_bins[1:]) / 2
            ny, nx = final_grid.shape

            for i in range(ny):
                for j in range(nx):
                    records.append({
                        "year": int(year),
                        "day": int(day),
                        "var": var,
                        "cell_i": i,
                        "cell_j": j,
                        "cell_id": f"{i}_{j}",
                        "x_center": x_centers[j],
                        "y_center": y_centers[i],
                        "value_interpolated": final_grid[i, j],
                        "value_raw": raw_grid[i, j]
                    })

    df = pd.DataFrame(records)
    df.to_pickle("/project/mgreenst/cloudseeding/intermediate/modis_panel_2020.pkl")

    print("DONE", flush=True)