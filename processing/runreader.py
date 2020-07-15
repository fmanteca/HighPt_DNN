import os
import shutil


for i in range(1,10):

    subcom = 'python reader.py --inputDir /gpfs/projects/cms/fernanpe/CRAB_PrivateMC/ZprimeToMuMu_M-5000_ntupler/200713_191405/0000/ --part ' + str(i)
    print 'part: ' + str(i)
    os.system(subcom)

 
 


                    


