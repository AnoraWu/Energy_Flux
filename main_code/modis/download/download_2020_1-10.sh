#!/bin/bash

TOKEN="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFub3JhYWFiaXUiLCJleHAiOjE3NzAyNDEyNTUsImlhdCI6MTc2NTA1NzI1NSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.Lj1HHLMqPv-JQSCkvaUt3L0Yp1GnY_QuaQBIIei-DkLbfhd0weILvjIyzlURXrmPKqdVKxjXt61tveA4f_DBciHVN-sIw01ZD0NB5NL52YqmpcMlzdeC76etO5CS8AXdX-JYoFE3GEh0w_ExBegGau-2yWtyHUAvmbRNkFGS2V_9OS6umErGOmeQVDfL6S9i0lg8FMBiCZ5iew19AH5ThEpPNW8AHv6sJfsvyDe8KF-m8044iZtBz1CRlFx0I9BYgxeWzyEaAjvxgy4i_Q20D_ZjEu5XP8j5fwclgu6hh1IdlRtMHmiS9DIbihcB99dbWjdJHekjHJsO9w4ZndeVDA"

OUT_DIR="/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/MODIS_L2"

for i in {001..10}; do
    echo "Downloading day: $i"

    wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 \
    "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD03/2020/$i/" \
    --header "Authorization: Bearer $TOKEN" -P "$OUT_DIR"
    
    wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 \
    "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/2020/$i/" \
    --header "Authorization: Bearer $TOKEN" -P "$OUT_DIR"
done