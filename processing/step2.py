import pandas as pd
import glob

all_files = glob.glob("/gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/STEP1/*.csv")

hits = []

for filename in all_files:
    df = pd.read_csv(filename)
    hits.append(df)

frame_hits = pd.concat(hits, ignore_index=True)



all_muonMeans = glob.glob("/gpfs/users/mantecap/CMSSW_9_4_4/src/MyAnalysis/DNN/processing/CLEANED_SEGMENTS/*.csv")

muonMeans = []

for filename in all_muonMeans:
    df = pd.read_csv(filename)
    muonMeans.append(df)

frame_muonMeans = pd.concat(muonMeans, ignore_index=True)


#TEST: 
#frame_hits.to_csv('test.csv', index=False)

muon_files = glob.glob("data/output_Muon_*.txt")

muons = []

for filename in muon_files:
    df = pd.read_csv(filename, sep='\t', header=None)
    muons.append(df)

frame_muons = pd.concat(muons, ignore_index=True)
frame_muons.columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid', 'Muon_nHits', 'Muon_nGeomDets', 'Muon_Genpt', 'Muon_InnerTrack_pt', 'Muon_InnerTrack_eta', 'Muon_InnerTrack_phi', 'Muon_InnerTrack_charge', 'Muon_InnerTrack_ptErr', 'Muon_InnerTrack_Chindf', 'Muon_TunePTrack_pt', 'Muon_TunePTrack_ptErr']


merge1 = pd.merge(frame_muons, frame_hits, on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid'])
output = pd.merge(merge1, frame_muonMeans, on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid'])
output.to_csv('../training/TrainFile.csv', index=False)

