#!/bin/bash
#SBATCH --job-name=check_download_2022
#SBATCH --output=/project/mgreenst/cloudseeding/check_download_2022.out
#SBATCH --error=/project/mgreenst/cloudseeding/check_download_2022.err

#SBATCH --account=pi-mgreenst
#SBATCH --partition=caslake
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=15

#SBATCH --mail-type=ALL
#SBATCH --mail-user=wanru@rcc.uchicago.edu

module load gcc/12.2.0

# load conda base from system Anaconda
source /software/python-anaconda-2022.05-el8-x86_64/etc/profile.d/conda.sh

conda activate r441

python "/project/mgreenst/cloudseeding/code/gridded_daily".py