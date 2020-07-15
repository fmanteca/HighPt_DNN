import os
import shutil


for i in range(1,1001):

    subcom = 'sbatch -o logfile.log -e errfile.err --qos=gridui_sort --partition=cloudcms merger.sh ' + str(i)
    os.system(subcom)

    # subcom = 'python merger.py --FileNumber ' + str(i)
    # print 'file: ' + str(i)
    # os.system(subcom)

 
 


                    


