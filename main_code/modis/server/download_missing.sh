#!/bin/bash
#SBATCH --job-name=download_2022_missing
#SBATCH --output=/project/mgreenst/cloudseeding/download_2022_missing.out
#SBATCH --error=/project/mgreenst/cloudseeding/download_2022_missing.err

#SBATCH --account=pi-mgreenst
#SBATCH --partition=caslake
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=12

#SBATCH --mail-type=ALL
#SBATCH --mail-user=wanru@rcc.uchicago.edu

module load parallel

TOKEN="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFub3JhYWFiaXUiLCJleHAiOjE3Njc0MTYyODAsImlhdCI6MTc2MjIzMjI4MCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.APCNFHIByotKczoMnep74D-nzzfVbd90XDITTQKa3SNgf9ovE1yvatgno7LdvqAVUkcTQeeyO0qLMm5W3IBn3PVV05GMh0rtjwZglVngnJzuqutJ-zoWOqcn_cUIx1UpR93Jv_iEL3WDktBZn8AET7J0FXjmL-N0iDj3TCjoRtQsS7ofALat5KY5kfvXPLs04MdMo7-K5kpTREgkR-hb9Gd5eAr3_MasFaQIuLbjDb3q3fdI7CdjjE5SLZGHnYcdevU2CHz7TqZ0e8hZQnWTdnrorYcK2zF_MuTrVciQvG1Zczogig_s24xVlNQ0M4z7iUgxHyvcHz-R2VcjP9kYCw"

LIST_FILE="/project/mgreenst/cloudseeding/check_download_2022.out"
RAWROOT="/project/mgreenst/cloudseeding/rawdata"
BASE_URL="https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61"

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

    # STEP 1: Get directory listing (HTML)
    HTML=$(curl -s --header "Authorization: Bearer $TOKEN" "$DIR_URL")

    # STEP 2: Extract matching filenames
    FILE=$(echo "$HTML" | grep -oE "${PREFIX}[^\"]+\.hdf" | head -1)

    if [[ -z "$FILE" ]]; then
        echo "No file matching $PREFIX found on server."
        echo "$PREFIX" >> missing_files.log
        return
    fi

    echo "Found remote file: $FILE"

    # STEP 3: Download the exact file
    wget --header "Authorization: Bearer $TOKEN" \
         -P "$DEST_DIR" \
         "${DIR_URL}${FILE}"
}

export -f download_one

# 并行 15 cores
parallel -j 15 --colsep ' ' download_one {1} {2} {3} {4} {5} :::: "$LIST_FILE"