#!/bin/bash
#SBATCH --job-name=clean_modis_2020_1-100
#SBATCH --output=/project/mgreenst/energy_flux/code/log/clean_modis_2020_1-100.out
#SBATCH --error=/project/mgreenst/energy_flux/code/log/clean_modis_2020_1-100.err

#SBATCH --account=pi-mgreenst
#SBATCH --partition=caslake
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

#SBATCH --mail-type=ALL
#SBATCH --mail-user=wanru@rcc.uchicago.edu

module load parallel

# load conda for running the check python script
source /software/python-anaconda-2022.05-el8-x86_64/etc/profile.d/conda.sh
conda activate r441

python /project/mgreenst/energy_flux/code/clean_modis_2020_1-100.py