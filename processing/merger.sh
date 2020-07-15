#!/bin/sh

cd /gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/
source /cvmfs/cms.cern.ch/cmsset_default.sh
eval `scramv1 runtime -sh`

python merger.py --FileNumber $1
