#!/bin/bash
#SBATCH --job-name=download_2020           # Name shown in squeue
#SBATCH --output=/project/mgreenst/energy_flux/code/log/download_2020_1_100.out  # Where stdout goes
#SBATCH --error=/project/mgreenst/energy_flux/code/log/download_2020_1_100.err   # Where stderr goes
#SBATCH --account=pi-mgreenst             # Allocation account to bill
#SBATCH --partition=caslake               # Which cluster partition to use
#SBATCH --time=24:00:00                   # Max wall time (job killed after this)
#SBATCH --nodes=1                         # Use 1 compute node
#SBATCH --ntasks=1                        # Run 1 main process (this script)
#SBATCH --cpus-per-task=4                 # Give it 4 CPU cores for parallel downloads
#SBATCH --mail-type=ALL                   # Email on job start, end, and failure
#SBATCH --mail-user=wanru@rcc.uchicago.edu  # Email address for notifications


# NASA Earthdata authentication token 
# Log in to Earthdata, go to https://urs.earthdata.nasa.gov/profile to generate
TOKEN="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFub3JhYWFiaXUiLCJleHAiOjE3Nzc1Njk2ODEsImlhdCI6MTc3MjM4NTY4MSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.hEJIIGtE7c_QqlZOVtZljQEKJYmU3M0_Glc9MRELYKR9mMQ7qnHgxibEdo2zImo-a9jmmpBZxvHYqxTOWpeHkOKo8UM33nWwEGCOwZK3x9Lw-Tu-MDTcJmiCpl2TXlhdLLPWwh4ntlsKsjv2RommQa0aOzNx16308Sh6GeBWz8HzAHxnGJZbqgRF75LrFDGfkIEC7rU4K0HafA_ZMPp2Kaq6VsF4U70UcEbuex_sFt1Wokq4TXcPXOGiQIJizDuDGsPml5H4Xqmmw7fGhQgh7MutyPRnHxLqVhink_CSYZCvqJZFGxEFzcjUfMGbWb4EM8cRyOHe7Wq24RuS2LWI3Q"

OUT_DIR="/project/mgreenst/energy_flux/modis_l2"

# Maximum number of parallel downloads running at the same time
# Too many parallel downloads will crash the process
MAX_JOBS=4


# Download function MOD03 and MOD06_L2 data for a single day-of-year
# Usage: download_day <day_number>   e.g. download_day 5
download_day() {
    local i=$1

    # Zero-pad the day number to 3 digits (e.g., 1 -> 001, 12 -> 012)
    local day
    day=$(printf "%03d" "$i")

    echo "Downloading day: $i (DOY: $day)"

    # Download data for this day
    # Command copied from https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD03/2020 - [see wdet download command]
    wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 \
        "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD03/2020/$day/" \
        --header "Authorization: Bearer $TOKEN" -P "$OUT_DIR" \
        && echo "MOD03 day $day done" || echo "MOD03 day $day FAILED"

    wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 \
        "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/2020/$day/" \
        --header "Authorization: Bearer $TOKEN" -P "$OUT_DIR" \
        && echo "MOD06_L2 day $day done" || echo "MOD06_L2 day $day FAILED"
}

# Export the function and variables so that background subshells
# (launched by &) can access them
export -f download_day
export TOKEN OUT_DIR

for i in $(seq 1 100); do

    # Launch the download for this day in the background (&)
    # This lets the loop continue immediately to start the next day
    download_day "$i" &

    # Check how many background jobs are currently running
    # If we've hit the limit (4), wait for any one to finish before starting more
    if (( $(jobs -rp | wc -l) >= MAX_JOBS )); then
        wait -n
    fi

done

# Wait for all remaining background jobs to finish before exiting
wait

echo "All downloads complete."