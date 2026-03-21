#!/bin/bash
#SBATCH --job-name=check_download_2020_1-100
#SBATCH --output=/project/mgreenst/energy_flux/code/log/check_download_2020_1-100.out
#SBATCH --error=/project/mgreenst/energy_flux/code/log/check_download_2020_1-100.err

#SBATCH --account=pi-mgreenst
#SBATCH --partition=caslake
#SBATCH --time=4:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=5

#SBATCH --mail-type=ALL
#SBATCH --mail-user=wanru@rcc.uchicago.edu

module load parallel

# load conda for running the check python script
source /software/python-anaconda-2022.05-el8-x86_64/etc/profile.d/conda.sh
conda activate r441

TOKEN="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFub3JhYWFiaXUiLCJleHAiOjE3Nzc1Njk2ODEsImlhdCI6MTc3MjM4NTY4MSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.hEJIIGtE7c_QqlZOVtZljQEKJYmU3M0_Glc9MRELYKR9mMQ7qnHgxibEdo2zImo-a9jmmpBZxvHYqxTOWpeHkOKo8UM33nWwEGCOwZK3x9Lw-Tu-MDTcJmiCpl2TXlhdLLPWwh4ntlsKsjv2RommQa0aOzNx16308Sh6GeBWz8HzAHxnGJZbqgRF75LrFDGfkIEC7rU4K0HafA_ZMPp2Kaq6VsF4U70UcEbuex_sFt1Wokq4TXcPXOGiQIJizDuDGsPml5H4Xqmmw7fGhQgh7MutyPRnHxLqVhink_CSYZCvqJZFGxEFzcjUfMGbWb4EM8cRyOHe7Wq24RuS2LWI3Q"

CHECK_SCRIPT="/project/mgreenst/energy_flux/code/check_download_2020_1-100.py"
RAWROOT="/project/mgreenst/energy_flux/modis_l2"
BASE_URL="https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61"

MAX_RETRIES=5

export TOKEN RAWROOT BASE_URL

download_one() {
    YEAR="$1"
    DAY="$2"
    HH="$3"
    MM="$4"
    PRODUCT="$5"

    PROD_UPPER=$(echo "$PRODUCT" | tr '[:lower:]' '[:upper:]')

    if [[ "$PROD_UPPER" == "MOD03" ]]; then
        PROD_DIR="MOD03"
        PREFIX="MOD03.A${YEAR}${DAY}.${HH}${MM}"
    elif [[ "$PROD_UPPER" == "MOD06" ]]; then
        PROD_DIR="MOD06_L2"
        PREFIX="MOD06_L2.A${YEAR}${DAY}.${HH}${MM}"
    else
        echo "Skipping unknown product: $PRODUCT"
        return
    fi

    DEST_DIR="${RAWROOT}/${PROD_DIR}/${YEAR}/${DAY}"
    mkdir -p "$DEST_DIR"

    DIR_URL="${BASE_URL}/${PROD_DIR}/${YEAR}/${DAY}/"

    echo "Searching for real file for prefix=$PREFIX ..."

    HTML=$(curl -s --header "Authorization: Bearer $TOKEN" "$DIR_URL")

    FILE=$(echo "$HTML" | grep -oE "${PREFIX}[^\"]+\.hdf" | head -1)

    if [[ -z "$FILE" ]]; then
        echo "No file matching $PREFIX found on server."
        echo "$PREFIX" >> missing_files.log
        return
    fi

    echo "Found remote file: $FILE"

    wget --header "Authorization: Bearer $TOKEN" \
         -P "$DEST_DIR" \
         "${DIR_URL}${FILE}"
}

export -f download_one

# ============ Main loop ============
ATTEMPT=0
MISSING_LIST="/project/mgreenst/energy_flux/code/log/missing_files_2020_1-100.txt"

while true; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "========== Attempt $ATTEMPT =========="

    # Step 1: Run check script, output to dedicated text file
    echo "Running check_download..."
    python "$CHECK_SCRIPT" > "$MISSING_LIST"

    # Backup this round's result
    cp "$MISSING_LIST" "${MISSING_LIST%.txt}_attempt${ATTEMPT}.txt"

    # Step 2: Count lines
    LINE_COUNT=$(wc -l < "$MISSING_LIST")
    echo "MISSING_LIST has $LINE_COUNT lines."

    if [[ "$LINE_COUNT" -le 2 ]]; then
        echo "All files downloaded. Done!"
        break
    fi

    # Step 3: Safety check
    if [[ "$ATTEMPT" -ge "$MAX_RETRIES" ]]; then
        echo "Reached max retries ($MAX_RETRIES). Stopping. $LINE_COUNT lines remaining."
        break
    fi

    # Step 4: Download missing files (skip first and last line)
    echo "Downloading missing files..."
    sed '1d;$d' "$MISSING_LIST" | parallel -j 4 --colsep ' ' download_one {1} {2} {3} {4} {5}

    echo "Download round $ATTEMPT finished. Re-checking..."
done




