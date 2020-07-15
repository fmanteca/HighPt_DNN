import ROOT
import argparse
from math import *
import os

# python reader.py --inputDir /gpfs/projects/cms/fernanpe/ZprimeToMuMu_M-5000_TuneCP5_13TeV-madgraphMLM-pythia8/ZprimeToMuMu_M-5000_ntupler/200630_000605/0000/ --part 1

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = "Receive the parameters")
    parser.add_argument('--inputDir', action = 'store', type = str, dest = 'inputDir', help = 'Define the inputDir path')
    parser.add_argument('--part', action = 'store', type = int, dest = 'part', help = 'files split due to memory problem [1,4]')

    args = parser.parse_args()

    variables_muons = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid', 'Muon_nHits', 'Muon_nGeomDets', 'Muon_Genpt', 'Muon_InnerTrack_pt', 'Muon_InnerTrack_eta', 'Muon_InnerTrack_phi', 'Muon_InnerTrack_charge', 'Muon_InnerTrack_ptErr', 'Muon_InnerTrack_Chindf', 'Muon_TunePTrack_pt', 'Muon_TunePTrack_ptErr']

    variables_hits = ['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid', 'Hit_Hitid', 'Hit_Detid', 'Hit_isDT', 'Hit_isCSC', 'Hit_DTstation', 'Hit_CSCstation', 'Hit_DetElement', 'Hit_x', 'Hit_y', 'Hit_z', 'Hit_distToProp', 'Hit_Compatibility', 'Hit_dirx', 'Hit_diry', 'Hit_dirz', 'Hit_chi2', 'Hit_ndof']

    variables_props = ['Prop_Eventid', 'Prop_EventluminosityBlock', 'Prop_Muonid', 'Prop_Detid', 'Prop_isDT', 'Prop_isCSC', 'Prop_DTstation', 'Prop_CSCstation', 'Prop_DetElement', 'Prop_x', 'Prop_y', 'Prop_z']

    counter = 0

    if args.part == 1:
        low = 0
        high = 100
    elif args.part == 2:
        low = 100
        high = 200
    elif args.part == 3:
        low = 200
        high = 300
    elif args.part == 4:
        low = 300
        high = 400
    elif args.part == 5:
        low = 400
        high = 500
    elif args.part == 6:
        low = 500
        high = 600
    elif args.part == 7:
        low = 600
        high = 700
    elif args.part == 8:
        low = 700
        high = 800
    elif args.part == 9:
        low = 800
        high = 900
    elif args.part == 10:
        low = 900
        high = 1001

    for File in os.listdir(args.inputDir)[low:high]:
    #print(os.listdir(args.inputDir))
    
    #for File in os.listdir(args.inputDir):

        out_hits = open('data/output_Hit_' + File.replace('tree_', '').replace('.root', '') + '.txt', 'w')
        out_muons = open('data/output_Muon_' + File.replace('tree_', '').replace('.root', '') + '.txt', 'w')
        out_props = open('data/output_Prop_' + File.replace('tree_', '').replace('.root', '') + '.txt', 'w')

        rootfile = ROOT.TFile.Open(args.inputDir+File, "READ")
        tree = rootfile.Get("Events")
            
        for event in tree:

            for entry in range(0,eval('event.' + variables_muons[0] + '.size()')):
                
                #print str(entry)
                line = str(eval('event.' + variables_muons[0] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[1] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[2] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[3] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[4] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[5] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[6] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[7] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[8] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[9] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[10] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[11] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[12] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_muons[13] + '.at(' + str(entry) + ')'))
                
                out_muons.write(line + '\n')


            for entry in range(0,eval('event.' + variables_props[0] + '.size()')):
                
                #print str(entry)
                line = str(eval('event.' + variables_props[0] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[1] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[2] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[3] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[4] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[5] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[6] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[7] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[8] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[9] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[10] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_props[11] + '.at(' + str(entry) + ')'))
                
                out_props.write(line + '\n')

                
            for entry in range(0,eval('event.' + variables_hits[0] + '.size()')):
                
                #print str(entry)
                line = str(eval('event.' + variables_hits[0] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[1] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[2] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[3] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[4] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[5] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[6] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[7] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[8] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[9] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[10] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[11] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[12] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[13] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[14] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[15] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[16] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[17] + '.at(' + str(entry) + ')'))  + '\t' + str(eval('event.' + variables_hits[18] + '.at(' + str(entry) + ')')) + '\t' + str(eval('event.' + variables_hits[19] + '.at(' + str(entry) + ')'))
                
                out_hits.write(line + '\n')
        


        out_hits.close()
        out_muons.close()
        out_props.close()
    
