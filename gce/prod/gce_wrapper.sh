#!/bin/bash

start_time=$(date)

#### Environment ####

project="stanleysfang"

gs_bucket="stanleysfang"
repository="gcp_audit"
code_path=$HOME/${repository}/gce/prod/
log_path=$HOME/${repository}/log/

export PATH="/home/stanleysfang92/anaconda3/bin:$PATH"

#### ts_2019_ncov_wrapper ####
source activate gcp_audit
python ${code_path}gce_dataprep.py 1>${log_path}gce_dataprep.out 2>&1

#### Run Time ####
end_time=$(date)
start=$(date -d "${start_time}" +%s)
end=$(date -d "${end_time}" +%s)
secs=$(($end-$start))

echo -e "\n================================"
echo -e "Script: ${0##/*/} $*"
echo -e "\nStart Time: ${start_time}"
echo -e "End Time: ${end_time}"
printf 'Run Time: %d day %d hr %d min %d sec\n' $(($secs/86400)) $(($secs%86400/3600)) $(($secs%3600/60)) $(($secs%60))
echo -e "================================"