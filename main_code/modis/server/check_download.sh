#!/bin/bash
#SBATCH --job-name=check_download_2022
#SBATCH --output=/project/mgreenst/cloudseeding/check_download_2022.out
#SBATCH --error=/project/mgreenst/cloudseeding/check_download_2022.err

#SBATCH --account=pi-mgreenst
#SBATCH --partition=caslake
#SBATCH --time=4:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=5

#SBATCH --mail-type=ALL
#SBATCH --mail-user=wanru@rcc.uchicago.edu

# load conda base from system Anaconda
source /software/python-anaconda-2022.05-el8-x86_64/etc/profile.d/conda.sh

conda activate r441
python /project/mgreenst/cloudseeding/code/check_download.py


