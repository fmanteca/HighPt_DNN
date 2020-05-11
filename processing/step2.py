import pandas as pd
import glob

all_files = glob.glob("/gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/STEP1/*.csv")

hits = []

for filename in all_files:
    df = pd.read_csv(filename)
    hits.append(df)

frame_hits = pd.concat(hits, ignore_index=True)


#TEST: 
#frame_hits.to_csv('test.csv', index=False)

muon_files = glob.glob("/gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/data/output_Muon_*.txt")

muons = []

for filename in muon_files:
    df = pd.read_csv(filename)
    muons.append(df)

frame_muons = pd.concat(muons, ignore_index=True)

#TEST: 
#frame_muons.to_csv('test.csv', index=False)


output = pd.merge(frame_muons, frame_hits, on=['Muon_Eventid','Muon_Muonid'])
output.to_csv('TrainFile.csv', index=False)

