## Data Processing Pipeline — Preliminary Analysis of Energy Flux 

**Author:** Anora Wu | **Contact:** AnoraaaBiu@outlook.com | **Updated:** May 2026

### Overview

This pipeline first cleans energy flux data (CERES SSF) and constructs a county-day panel data on cloud seeding operations and energy flux variables, covering counties in China. Then, it tests whether cloud seeding operations affect cloud and energy flux outcomes using propensity-score matching (PSM) and event-study designs.

---

### Step 0: Data Preparation
`ssf_clean.py` — Reads raw CERES satellite NetCDF files (solar/radiation flux data), spatially joins each pixel to a county via a shapefile, converts UTC→Beijing time, drops nighttime flux readings (solar zenith ≥ 90°), normalizes flux by cosine of solar zenith, aggregates to county-day means, and writes combined_ssf.csv. 

`ssf_merge_cloudseeding.do` — Loads that CSV, collapses duplicate county-day rows, drops an unmatched region code and dates after Sep 2024, then merges it 1:1 onto the main skeleton_merged2024.dta panel (which has cloud-seeding operation records). Renames/relabels all the CERES variable columns into readable names. Produces skeleton_merged2024_ssf.dta.


### Step 1: Propensity score estimation
`pscore.do` — Build 7-day lags of cloud thickness/fraction/velocity/air-fraction, run a logit of the seeding indicator (imply) on those lags, predict a propensity score for each county-day. 

`pscore_nofraction.do` / `pscore_nothick.do` — Same logit approach but leaving out thickness or fraction respectively, as thickness or fraction will be used in propensity-score matching later.

### Step 2: Matching pipelines (treated vs. control) and Event Study
`psm_basic_match.do` — Interpolates flux/radiance series (fills gaps ≤10 days), builds lagged rain-forecast variables, exact-matches treated seeding events to control days on forecast/rain history + nearest propensity score, expands each matched pair into a ‑7…+10 day event window, then runs reghdfe event-study regressions and exports coefficient plots for flux/thickness/fraction outcomes.

`psm_match_y_cloud.do` — Same matching logic but also exact-matches on rounded pre-treatment cloud thickness/fraction values (using the "nofraction"/"nothickness" pscores), then produces event-study plots plus CDF and Q-Q diagnostic plots comparing treated/control distributions pre/post treatment.

`psm_match_y_cos.do` — Same pipeline for zenith-angle-normalized flux variables (sw_toa_flux_up_norm, net_sw_surface_flux_norm).

`psm_match_y_flux.do` — Same pipeline for raw (non-normalized) flux/radiance variables (SW/LW TOA flux, surface flux, radiance).

`psm_match_y_resid.do` — Regresses each flux variable on solar zenith and keeps the residuals (removing the zenith-angle effect) before running the same matching + event-study pipeline as the others. Also produces zenith-vs-flux scatter diagnostics.

`event study flux.do` — Builds a cohort/never-treated event-time panel directly from the skeleton data (no PSM matching), restricts to a ‑7…+10 day window, and runs simple reghdfe event-study regressions + coefficient plots for thickness, fraction, and various flux/radiance variables.

### Step 3: Visualization
`psm_match_y_rain.do` — Not a matching script itself; just draws pre/post, treated/control histogram panels of rainfall.

**Note**: Please refer to `Preliminary_Analysis.tex` in the Energy_Flux project on Overleaf for outputs.




