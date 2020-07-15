import pandas as pd
import glob
import argparse
import os

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = "Receive the parameters")
    parser.add_argument('--FileNumber', action = 'store', type = str, dest = 'FileNumber', help = 'Which file number to take as input')

    args = parser.parse_args()

    hits_files = glob.glob("data/output_Hit_" + args.FileNumber + ".txt")

    hits = []

    for filename in hits_files:
        df = pd.read_csv(filename, sep='\t', header=None)
        hits.append(df)

    frame_hits = pd.concat(hits, ignore_index=True)
    frame_hits.columns = ['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid', 'Hit_Hitid', 'Hit_Detid', 'Hit_isDT', 'Hit_isCSC', 'Hit_DTstation', 'Hit_CSCstation', 'Hit_DetElement', 'Hit_x', 'Hit_y', 'Hit_z', 'Hit_distToProp', 'Hit_Compatibility', 'Hit_dirx', 'Hit_diry', 'Hit_dirz', 'Hit_chi2', 'Hit_ndof']

    #TEST: 
    #frame_hits.to_csv('test.csv', index=False)

    muon_files = glob.glob("data/output_Muon_" + args.FileNumber + ".txt")

    muons = []

    for filename in muon_files:
        df = pd.read_csv(filename, sep='\t', header=None)
        muons.append(df)

    frame_muons = pd.concat(muons, ignore_index=True)
    frame_muons.columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid', 'Muon_nHits', 'Muon_nGeomDets', 'Muon_Genpt', 'Muon_InnerTrack_pt', 'Muon_InnerTrack_eta', 'Muon_InnerTrack_phi', 'Muon_InnerTrack_charge', 'Muon_InnerTrack_ptErr', 'Muon_InnerTrack_Chindf', 'Muon_TunePTrack_pt', 'Muon_TunePTrack_ptErr']

    #TEST: 
    #frame_muons.to_csv('test.csv', index=False)


    output = pd.merge(frame_muons, frame_hits, left_on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid'], right_on=['Hit_Eventid','Hit_EventluminosityBlock','Hit_Muonid'])
    #output.to_csv('data_hit_muon.csv', index=False)



    prop_files = glob.glob("data/output_Prop_" + args.FileNumber + ".txt")

    props = []

    for filename in prop_files:
        df = pd.read_csv(filename, sep='\t', header=None)
        props.append(df)

    frame_props = pd.concat(props, ignore_index=True)
    frame_props.columns = ['Prop_Eventid', 'Prop_EventluminosityBlock', 'Prop_Muonid', 'Prop_Detid', 'Prop_isDT', 'Prop_isCSC', 'Prop_DTstation', 'Prop_CSCstation', 'Prop_DetElement', 'Prop_x', 'Prop_y', 'Prop_z']

    output_all = pd.merge(output, frame_props, left_on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid', 'Hit_Detid'], right_on=['Prop_Eventid','Prop_EventluminosityBlock','Prop_Muonid', 'Prop_Detid'])
    output_all.to_csv('MERGER/data_hit_muon_prop' + args.FileNumber + '.csv', index=False)
    
    del df
    del frame_hits
    del frame_muons
    del frame_props
    del output
    del output_all
