## Data Processing Pipeline — Jiangxi Cloud Seeding Study

**Author:** Anora Wu | **Contact:** AnoraaaBiu@outlook.com | **Updated:** May 2026

### Overview

This pipeline constructs a spatially-gridded panel dataset covering Jiangxi Province, China (2020–2025) to study the effects of cloud seeding operations on cloud properties. It links administrative cloud seeding operation records with MODIS satellite observations on a common 5 km × 5 km spatial grid at hourly resolution.

The entry point is `master.sh`. Before running, set `DATA_DIR` to your local Dropbox path and update the `cd` path to your repository clone.

---

### Step 0: `jiangxi_shape.py`

The raw administrative shapefile covers all townships in China. This script dissolves those township polygons into a single unified boundary for Jiangxi Province, which is our target of research. 

---

### Step 1: `clean_town_operation.py`

Cloud seeding operation records span six years (2020–2025) but arrive in inconsistent Excel formats with different column schemas. This script standardizes them into a single dataset and spatially joins each operation to its containing township using the Jiangxi shapefile. The spatial join (rather than relying on text fields in the records) prevents errors from ambiguous or misspelled place names. An optional diagnostic histogram (`CHECK=True`) verifies that operation counts are plausible across years.

---

### Step 2: `merge_town_to_grid.py`

This step defines the panel structure that everything else aligns to. The Jiangxi boundary is projected to UTM Zone 50N (EPSG:32650) so that the 5 km grid can be constructed in meters. Each grid cell receives an ID based on its position in the grid.

Operation records are mapped into this grid and aggregated to hourly counts per cell. Because a seeding operation spans a time window, two versions are produced for each year: one using the operation's start hour and one using its end hour. This allows downstream analysis to test different assumptions about when seeding takes effect. Each year's panel is saved separately to keep memory use manageable.

---

### Step 3: Server-side MODIS Processing (`server_example_2020_1-100/`)

MODIS Level-2 swath data is too large to process locally. These steps run on the UChicago RCC cluster. The scripts in `server_example_2020_1-100/` cover days 1–100 of 2020 as an example; the full set of scripts for all dates is at `/project/mgreenst/energy_flux/code/` on the server.

**Download** (`download_2020_1-100.sh`): Fetches MOD06_L2 (cloud properties) and MOD03 (geolocation) files from NASA for the target dates.

**Verify** (`check_download_2020_1-100.sh`): Downloads are sometimes silently incomplete. This script checks that every expected file arrived and logs any gaps.

**Extract** (`clean_modis_2020_1-100.sh`): Reads the MODIS files and converts the relevant cloud variables into flat CSV files keyed by date.

**Validate fill values** (`check_cloud_top_fill_2025.sh`): Per the MODIS 6.1 User Guide (p. 89, in Dropbox at `Energy_Flux/modis_l2/Documentation/`), cloud optical thickness retrievals whose corresponding 1 km cloud-top temperature or pressure is set to a fill value should be discarded. This script confirms those entries are absent. Checks were run for 2024–2025; server logs confirm no such entries survive in those years.

---

### Step 4: `merge_modis_to_grid_unfiltered.py`

This script merges the processed MODIS CSV files onto the panel created in Step 2. MODIS pixels are mapped to grid cells using the same coordinate formula as the operations (EPSG:32650, 5 km bins), so the spatial indexing is consistent. Multiple pixels falling in the same cell and hour are averaged. Both 1 km (cloud optical thickness) and 5 km (cloud fraction) variables are joined.

**Note on `merge_modis_to_grid.py`:** An earlier version of this script filtered MODIS cloud fraction to pixels flagged as "useful" by the QA bitmask. That filter was found to introduce systematic biases and has been removed. QA filtering for cloud optical thickness has also been removed: those QA flags overlap with fill values, which were already discarded in the validation step above (Step 3). For the full methodological details and evidence, see `Cloud_Fraction_Report.tex` in the Energy_Flux project on Overleaf.
