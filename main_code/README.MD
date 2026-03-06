## Data Processing Pipeline — Jiangxi Cloud Seeding Study

**Author:** Anora Wu | **Updated:** Feb 2026

### Overview

This pipeline constructs a spatially-gridded panel dataset covering Jiangxi Province, China (2020–2025) to study the effects of cloud seeding operations. It combines administrative operation records with satellite-derived cloud property data on a common 5 km × 5 km spatial grid.

---

### Step 1: Clean Cloud Seeding Operation Records (`clean_town_operation.py`)

- **Input:** Annual Excel files of cloud seeding operations (2020–2025).
- **Process:** Standardizes column names across two distinct formats (2020 vs. 2021–2025). Performs a spatial join against a township-level shapefile to assign each operation to its corresponding township in Jiangxi Province.
- **Output:** `intermediate/cleaned_operation.csv` — a unified, cleaned record of all operations with each operation's latitude, longitude, and other information.

---

### Step 2: Construct the Spatial Grid and Merge Operations (`merge_town_to_grid.py`)

- **Input:** Jiangxi province shapefile; `cleaned_operation.csv`.
- **Process:**
  1. Projects the Jiangxi boundary to UTM Zone 50N (EPSG:32650) and subdivides it into a 5 km × 5 km grid, retaining only cells that intersect the province.
  2. Creates a balanced panel: every combination of grid cell × calendar date from 2020-01-01 to 2025-12-31.
  3. Geocodes each operation record to its grid cell and counts the number of operations per cell per day.
  4. Optionally generates diagnostic plots (grid map, spatial distribution of operations).
- **Output:**
  - `intermediate/jx_grid.gpkg` / `jx_grid.csv` — the grid geometry and identifiers.
  - `intermediate/grid_with_operation.csv` — the full panel with daily operation counts per grid cell.

---

### Step 3: Process MODIS Satellite Cloud Data (`merge_modis_to_grid.py`)

- **Input:** MODIS Level-2 MOD06\_L2 HDF4 swath files; MOD03 geolocation files for 1 km products.
- **Process:**
  1. Reads raw Level-2 swath data for two resolutions:
     - **5 km:** Cloud Fraction, Cloud Fraction Day/Night.
     - **1 km:** Cloud Optical Thickness and its uncertainty (requires paired MOD03 geolocation files; filters by retrieval quality, cloud phase, and confidence flags from the QA bitmask).
  2. Projects pixel coordinates to EPSG:32650 and bins observations onto the same 5 km grid using 2D histograms, computing daily mean values per cell.
  3. Interpolates residual within-province gaps using linear spatial interpolation; masks all pixels outside the Jiangxi boundary.
  4. Parallelizes processing across all days and variables using Python `multiprocessing.Pool`, configured for SLURM HPC environments.
- **Output:** `intermediate/modis_panel_YYYY.pkl` — a serialized DataFrame with daily, gridded MODIS cloud variables (raw and gap-filled) aligned to the same `cell_id` index as the operation panel.

---

### Pipeline (`master.sh`)

A shell script sets the data directory, optionally enables diagnostic checks, and runs the three steps in sequence. It is designed to be reproducible by other users by changing only the `DATA_DIR` and working directory variables.
