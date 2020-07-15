import os
import shutil


for i in range(1,1001):

    # f = open('step1_' + str(i) + '.sh', 'w')
    # text = '#!/bin/sh\n'
    # text += 'cd /gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/\n'
    # text += 'source /cvmfs/cms.cern.ch/cmsset_default.sh\n'
    # text += 'eval `scramv1 runtime -sh`\n'
    # text += 'python doStep1.py --FileNumber ' + str(i) + '\n' 
    # f.write(text)
    # f.close()

    subcom = 'sbatch -o logfile.log -e errfile.err --qos=gridui_sort --partition=cloudcms step1.sh ' + str(i)
    os.system(subcom)

    #os.remove('submit_' + str(i)  + '.sh')
 
 


                    


