import numpy as np
import pandas as pd
import argparse

# python doStep1.py --FileNumber 1

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = "Receive the parameters")
    parser.add_argument('--FileNumber', action = 'store', type = str, dest = 'FileNumber', help = 'Which file number to take as input')

    args = parser.parse_args()
    
    # STEP1: FIRST I MERGE THE HITS AND PROPS FRAMES

    hits = pd.read_csv('data/output_Hit_' + args.FileNumber + '.txt', sep="\t", header=None)
    hits.columns = ['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid', 'Hit_Hitid', 'Hit_Detid', 'Hit_isDT', 'Hit_isCSC', 'Hit_DTstation', 'Hit_CSCstation', 'Hit_DetElement', 'Hit_x', 'Hit_y', 'Hit_z', 'Hit_distToProp', 'Hit_Compatibility', 'Hit_dirx', 'Hit_diry', 'Hit_dirz', 'Hit_chi2', 'Hit_ndof']
    

    props = pd.read_csv('data/output_Prop_' + args.FileNumber + '.txt', sep="\t", header=None)
    props.columns = ['Prop_Eventid', 'Prop_EventluminosityBlock', 'Prop_Muonid', 'Prop_Detid', 'Prop_isDT', 'Prop_isCSC', 'Prop_DTstation', 'Prop_CSCstation', 'Prop_DetElement', 'Prop_x', 'Prop_y', 'Prop_z']


    data = pd.merge(hits, props, left_on=['Hit_Eventid','Hit_EventluminosityBlock','Hit_Muonid', 'Hit_Detid'], right_on=['Prop_Eventid','Prop_EventluminosityBlock','Prop_Muonid', 'Prop_Detid'])


    data_DT = data[(data.Hit_isDT == 1)]
    data_CSC = data[(data.Hit_isCSC == 1)]
    
    # STEP2: CLEAR BAD PROPAGATIONS & REMOVE DUPLICATED HITS

    data_DT.loc[:, 'prop_Rxy'] = np.sqrt(data_DT.Prop_x*data_DT.Prop_x + data_DT.Prop_y*data_DT.Prop_y)
    data_CSC.loc[:, 'prop_Rxy'] = np.sqrt(data_CSC.Prop_x*data_CSC.Prop_x + data_CSC.Prop_y*data_CSC.Prop_y)

    data_DT = data_DT.drop(data_DT[(data_DT.prop_Rxy>810.)].index)
    data_DT = data_DT.drop(data_DT[(data_DT.prop_Rxy > 470.) & (data_DT.prop_Rxy < 490.)].index)
    data_DT = data_DT.drop(data_DT[(data_DT.prop_Rxy > 555.) &  (data_DT.prop_Rxy < 590.)].index)
    data_DT = data_DT.drop(data_DT[(data_DT.prop_Rxy > 670.) & (data_DT.prop_Rxy < 690.)].index)

    data_CSC = data_CSC[(data_CSC.prop_Rxy<720.)]

    #Drop duplicates:

    data_DT = data_DT.groupby(['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid', 'Hit_x', 'Hit_y', 'Hit_z'], as_index=False).first()
    data_CSC = data_CSC.groupby(['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid', 'Hit_x', 'Hit_y', 'Hit_z'], as_index=False).first()

    #SAVE THE CLEANED SEGMENTS LIST

    cleaned_data = pd.concat([data_DT,data_CSC],ignore_index=True)
    cleaned_data.loc[:, 'Hit_chi2_div_ndof'] = (cleaned_data.Hit_chi2 / cleaned_data.Hit_ndof)

    data_grouped_perMuon = cleaned_data.groupby(['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid'], as_index=False)
    output_mean = data_grouped_perMuon.mean()[['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid','Hit_dirx','Hit_diry','Hit_dirz','Hit_Compatibility', 'Hit_chi2_div_ndof']]

    output_mean.columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_mean_dirx','Muon_mean_diry','Muon_mean_dirz','Muon_mean_Compatibility','Muon_mean_chi2_div_ndof']
    output_mean.to_csv('CLEANED_SEGMENTS/cleaned_mean_' + args.FileNumber + '.csv', index=False)
    
    data_DT.drop(data_DT.columns[14:], axis=1, inplace=True)
    data_CSC.drop(data_CSC.columns[14:], axis=1, inplace=True)



    # STEP3: GET THE NUMBER OF HITS PER MUON AND PER MUON STATION

    # If there is not any hit in one station (for each muon in the event), then add hit entry with all the values set to 0.

    stations_list = [1,2,3,4]

    for event in data.Hit_Eventid.unique():
        for lumi in data[(data.Hit_Eventid == event)].Hit_EventluminosityBlock.unique():
            for muon in data[(data.Hit_Eventid == event) & (data.Hit_EventluminosityBlock == lumi)].Hit_Muonid.unique():
                muon_stations = data_DT[(data_DT.Hit_Eventid == event) & (data_DT.Hit_EventluminosityBlock == lumi) & (data_DT.Hit_Muonid == muon)].Hit_DTstation.unique()
                for i in stations_list:
                    if not i in muon_stations:
                        data_DT = data_DT.append(pd.DataFrame([[event,lumi,muon,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,i,np.nan,np.nan,np.nan]], columns=data_DT.columns))

    group_DT = data_DT.groupby(['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid','Hit_DTstation'], as_index=False)

    muon_nHits_perDTStation = group_DT.count()[['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid','Hit_DTstation','Hit_isDT']].fillna(0) 
    muon_Mean_perDTStation = group_DT.mean()[['Hit_x','Hit_y','Hit_z']].fillna(0.) #fillna(0) for nhits=0 cases 
    muon_Std_perDTStation = group_DT.std()[['Hit_x','Hit_y','Hit_z']].fillna(0.)   #fillna(0) for nhits=1 || nhits=0 cases 
    

    # Add skew and kurtosis
    
    skew_info = []
    kurt_info = []

    for group in group_DT.groups: 
        temp = data_DT[(data_DT.Hit_Eventid == group[0]) & (data_DT.Hit_EventluminosityBlock == group[1]) & (data_DT.Hit_Muonid == group[2]) & (data_DT.Hit_DTstation == group[3])]
        skew = temp.skew(axis = 0)[['Hit_x', 'Hit_y', 'Hit_z']]
        kurt = temp.kurt(axis = 0)[['Hit_x', 'Hit_y', 'Hit_z']]
        skew_info.append({"Hit_x_skew":skew['Hit_x'],"Hit_y_skew":skew['Hit_y'], "Hit_z_skew":skew['Hit_z']})
        kurt_info.append({"Hit_x_kurt":kurt['Hit_x'],"Hit_y_kurt":kurt['Hit_y'], "Hit_z_kurt":kurt['Hit_z']})


    muon_Skew_perDTStation = pd.DataFrame(skew_info).fillna(0.)
    muon_Kurt_perDTStation = pd.DataFrame(kurt_info).fillna(0.)


    # DO THE SAME FOR THE CSC HITS

    for event in data.Hit_Eventid.unique():
        for lumi in data[(data.Hit_Eventid == event)].Hit_EventluminosityBlock.unique():
            for muon in data[(data.Hit_Eventid == event) & (data.Hit_EventluminosityBlock == lumi)].Hit_Muonid.unique():
                muon_stations = data_CSC[(data_CSC.Hit_Eventid == event) & (data_CSC.Hit_EventluminosityBlock == lumi) & (data_CSC.Hit_Muonid == muon)].Hit_CSCstation.unique()
                for i in stations_list:
                    if not i in muon_stations:
                        data_CSC = data_CSC.append(pd.DataFrame([[event,lumi,muon,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,i,np.nan,np.nan]], columns=data_CSC.columns))


    group_CSC = data_CSC.groupby(['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid','Hit_CSCstation'], as_index=False)

    muon_nHits_perCSCStation = group_CSC.count()[['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid','Hit_CSCstation','Hit_isCSC']].fillna(0) 
    muon_Mean_perCSCStation = group_CSC.mean()[['Hit_x','Hit_y','Hit_z']].fillna(0.) #fillna(0) for nhits=0 cases 
    muon_Std_perCSCStation = group_CSC.std()[['Hit_x','Hit_y','Hit_z']].fillna(0.)   #fillna(0) for nhits=1 || nhits=0 cases 


    # Add skew and kurtosis
    
    skew_info = []
    kurt_info = []

    for group in group_CSC.groups: 
        temp = data_CSC[(data_CSC.Hit_Eventid == group[0]) & (data_CSC.Hit_EventluminosityBlock == group[1]) & (data_CSC.Hit_Muonid == group[2]) & (data_CSC.Hit_CSCstation == group[3])]
        skew = temp.skew(axis = 0)[['Hit_x', 'Hit_y', 'Hit_z']]
        kurt = temp.kurt(axis = 0)[['Hit_x', 'Hit_y', 'Hit_z']]
        skew_info.append({"Hit_x_skew":skew['Hit_x'],"Hit_y_skew":skew['Hit_y'], "Hit_z_skew":skew['Hit_z']})
        kurt_info.append({"Hit_x_kurt":kurt['Hit_x'],"Hit_y_kurt":kurt['Hit_y'], "Hit_z_kurt":kurt['Hit_z']})


    muon_Skew_perCSCStation = pd.DataFrame(skew_info).fillna(0.)
    muon_Kurt_perCSCStation = pd.DataFrame(kurt_info).fillna(0.)

    del temp, skew, kurt, skew_info, kurt_info


    # ADD VARIABLES TO MUON_* TABLE -> SUM, MEAN, AND STD OF HITS IN EACH MUON STATION

    mergedDT = (muon_nHits_perDTStation.join(muon_Mean_perDTStation)).join(muon_Std_perDTStation,lsuffix='_mean', rsuffix='_std').join(muon_Skew_perDTStation).join(muon_Kurt_perDTStation)
    
    mergedDT_pivoted = pd.pivot_table(mergedDT, index=['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid'], columns='Hit_DTstation')


    Muon_DT_s1 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_DT_s1_nhits', 'Muon_DT_s1_x_mean', 'Muon_DT_s1_y_mean', 'Muon_DT_s1_z_mean',  'Muon_DT_s1_x_std', 'Muon_DT_s1_y_std', 'Muon_DT_s1_z_std', 'Muon_DT_s1_x_skew', 'Muon_DT_s1_y_skew', 'Muon_DT_s1_z_skew', 'Muon_DT_s1_x_kurt', 'Muon_DT_s1_y_kurt', 'Muon_DT_s1_z_kurt'])
    Muon_DT_s2 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_DT_s2_nhits', 'Muon_DT_s2_x_mean', 'Muon_DT_s2_y_mean', 'Muon_DT_s2_z_mean',  'Muon_DT_s2_x_std', 'Muon_DT_s2_y_std', 'Muon_DT_s2_z_std', 'Muon_DT_s2_x_skew', 'Muon_DT_s2_y_skew', 'Muon_DT_s2_z_skew', 'Muon_DT_s2_x_kurt', 'Muon_DT_s2_y_kurt', 'Muon_DT_s2_z_kurt'])
    Muon_DT_s3 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_DT_s3_nhits', 'Muon_DT_s3_x_mean', 'Muon_DT_s3_y_mean', 'Muon_DT_s3_z_mean',  'Muon_DT_s3_x_std', 'Muon_DT_s3_y_std', 'Muon_DT_s3_z_std', 'Muon_DT_s3_x_skew', 'Muon_DT_s3_y_skew', 'Muon_DT_s3_z_skew', 'Muon_DT_s3_x_kurt', 'Muon_DT_s3_y_kurt', 'Muon_DT_s3_z_kurt'])
    Muon_DT_s4 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_DT_s4_nhits', 'Muon_DT_s4_x_mean', 'Muon_DT_s4_y_mean', 'Muon_DT_s4_z_mean',  'Muon_DT_s4_x_std', 'Muon_DT_s4_y_std', 'Muon_DT_s4_z_std', 'Muon_DT_s4_x_skew', 'Muon_DT_s4_y_skew', 'Muon_DT_s4_z_skew', 'Muon_DT_s4_x_kurt', 'Muon_DT_s4_y_kurt', 'Muon_DT_s4_z_kurt'])

    
    

    for event in mergedDT.Hit_Eventid.unique():
        for lumi in mergedDT[(mergedDT.Hit_Eventid == event)].Hit_EventluminosityBlock.unique():
            for muon in mergedDT[(mergedDT.Hit_Eventid == event) & (mergedDT.Hit_EventluminosityBlock == lumi)].Hit_Muonid.unique():
                for i in stations_list:
                    Muon_DT_nhits = mergedDT_pivoted['Hit_isDT'][i].loc[(event, lumi, muon)]
                    Muon_DT_x_mean = mergedDT_pivoted['Hit_x_mean'][i].loc[(event, lumi, muon)]
                    Muon_DT_y_mean = mergedDT_pivoted['Hit_y_mean'][i].loc[(event, lumi, muon)]
                    Muon_DT_z_mean = mergedDT_pivoted['Hit_z_mean'][i].loc[(event, lumi, muon)]
                    Muon_DT_x_std = mergedDT_pivoted['Hit_x_std'][i].loc[(event, lumi, muon)]
                    Muon_DT_y_std = mergedDT_pivoted['Hit_y_std'][i].loc[(event, lumi, muon)]
                    Muon_DT_z_std = mergedDT_pivoted['Hit_z_std'][i].loc[(event, lumi, muon)]
                    Muon_DT_x_skew = mergedDT_pivoted['Hit_x_skew'][i].loc[(event, lumi, muon)]
                    Muon_DT_y_skew = mergedDT_pivoted['Hit_y_skew'][i].loc[(event, lumi, muon)]
                    Muon_DT_z_skew = mergedDT_pivoted['Hit_z_skew'][i].loc[(event, lumi, muon)]
                    Muon_DT_x_kurt = mergedDT_pivoted['Hit_x_kurt'][i].loc[(event, lumi, muon)]
                    Muon_DT_y_kurt = mergedDT_pivoted['Hit_y_kurt'][i].loc[(event, lumi, muon)]
                    Muon_DT_z_kurt = mergedDT_pivoted['Hit_z_kurt'][i].loc[(event, lumi, muon)]
                    if i == 1:
                        Muon_DT_s1 = Muon_DT_s1.append(pd.DataFrame([[event,lumi,muon,Muon_DT_nhits,Muon_DT_x_mean,Muon_DT_y_mean,Muon_DT_z_mean,Muon_DT_x_std,Muon_DT_y_std,Muon_DT_z_std,Muon_DT_x_skew,Muon_DT_y_skew,Muon_DT_z_skew,Muon_DT_x_kurt,Muon_DT_y_kurt,Muon_DT_z_kurt]], columns=Muon_DT_s1.columns))
                    elif i == 2:
                        Muon_DT_s2 = Muon_DT_s2.append(pd.DataFrame([[event,lumi,muon,Muon_DT_nhits,Muon_DT_x_mean,Muon_DT_y_mean,Muon_DT_z_mean,Muon_DT_x_std,Muon_DT_y_std,Muon_DT_z_std,Muon_DT_x_skew,Muon_DT_y_skew,Muon_DT_z_skew,Muon_DT_x_kurt,Muon_DT_y_kurt,Muon_DT_z_kurt]], columns=Muon_DT_s2.columns))
                    elif i == 3:
                        Muon_DT_s3 = Muon_DT_s3.append(pd.DataFrame([[event,lumi,muon,Muon_DT_nhits,Muon_DT_x_mean,Muon_DT_y_mean,Muon_DT_z_mean,Muon_DT_x_std,Muon_DT_y_std,Muon_DT_z_std,Muon_DT_x_skew,Muon_DT_y_skew,Muon_DT_z_skew,Muon_DT_x_kurt,Muon_DT_y_kurt,Muon_DT_z_kurt]], columns=Muon_DT_s3.columns))
                    elif i == 4:
                        Muon_DT_s4 = Muon_DT_s4.append(pd.DataFrame([[event,lumi,muon,Muon_DT_nhits,Muon_DT_x_mean,Muon_DT_y_mean,Muon_DT_z_mean,Muon_DT_x_std,Muon_DT_y_std,Muon_DT_z_std,Muon_DT_x_skew,Muon_DT_y_skew,Muon_DT_z_skew,Muon_DT_x_kurt,Muon_DT_y_kurt,Muon_DT_z_kurt]], columns=Muon_DT_s4.columns))

     



    mergedCSC = (muon_nHits_perCSCStation.join(muon_Mean_perCSCStation)).join(muon_Std_perCSCStation,lsuffix='_mean', rsuffix='_std').join(muon_Skew_perCSCStation).join(muon_Kurt_perCSCStation)
    
    mergedCSC_pivoted = pd.pivot_table(mergedCSC, index=['Hit_Eventid', 'Hit_EventluminosityBlock', 'Hit_Muonid'], columns='Hit_CSCstation')


    Muon_CSC_s1 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_CSC_s1_nhits', 'Muon_CSC_s1_x_mean', 'Muon_CSC_s1_y_mean', 'Muon_CSC_s1_z_mean',  'Muon_CSC_s1_x_std', 'Muon_CSC_s1_y_std', 'Muon_CSC_s1_z_std', 'Muon_CSC_s1_x_skew', 'Muon_CSC_s1_y_skew', 'Muon_CSC_s1_z_skew', 'Muon_CSC_s1_x_kurt', 'Muon_CSC_s1_y_kurt', 'Muon_CSC_s1_z_kurt'])
    Muon_CSC_s2 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_CSC_s2_nhits', 'Muon_CSC_s2_x_mean', 'Muon_CSC_s2_y_mean', 'Muon_CSC_s2_z_mean',  'Muon_CSC_s2_x_std', 'Muon_CSC_s2_y_std', 'Muon_CSC_s2_z_std', 'Muon_CSC_s2_x_skew', 'Muon_CSC_s2_y_skew', 'Muon_CSC_s2_z_skew', 'Muon_CSC_s2_x_kurt', 'Muon_CSC_s2_y_kurt', 'Muon_CSC_s2_z_kurt'])
    Muon_CSC_s3 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_CSC_s3_nhits', 'Muon_CSC_s3_x_mean', 'Muon_CSC_s3_y_mean', 'Muon_CSC_s3_z_mean',  'Muon_CSC_s3_x_std', 'Muon_CSC_s3_y_std', 'Muon_CSC_s3_z_std', 'Muon_CSC_s3_x_skew', 'Muon_CSC_s3_y_skew', 'Muon_CSC_s3_z_skew', 'Muon_CSC_s3_x_kurt', 'Muon_CSC_s3_y_kurt', 'Muon_CSC_s3_z_kurt'])
    Muon_CSC_s4 = pd.DataFrame(columns = ['Muon_Eventid', 'Muon_EventluminosityBlock', 'Muon_Muonid','Muon_CSC_s4_nhits', 'Muon_CSC_s4_x_mean', 'Muon_CSC_s4_y_mean', 'Muon_CSC_s4_z_mean',  'Muon_CSC_s4_x_std', 'Muon_CSC_s4_y_std', 'Muon_CSC_s4_z_std', 'Muon_CSC_s4_x_skew', 'Muon_CSC_s4_y_skew', 'Muon_CSC_s4_z_skew', 'Muon_CSC_s4_x_kurt', 'Muon_CSC_s4_y_kurt', 'Muon_CSC_s4_z_kurt'])

    
    

    for event in mergedCSC.Hit_Eventid.unique():
        for lumi in mergedCSC[(mergedCSC.Hit_Eventid == event)].Hit_EventluminosityBlock.unique():
            for muon in mergedCSC[(mergedCSC.Hit_Eventid == event) & (mergedCSC.Hit_EventluminosityBlock == lumi)].Hit_Muonid.unique():
                for i in stations_list:
                    Muon_CSC_nhits = mergedCSC_pivoted['Hit_isCSC'][i].loc[(event, lumi, muon)]
                    Muon_CSC_x_mean = mergedCSC_pivoted['Hit_x_mean'][i].loc[(event, lumi, muon)]
                    Muon_CSC_y_mean = mergedCSC_pivoted['Hit_y_mean'][i].loc[(event, lumi, muon)]
                    Muon_CSC_z_mean = mergedCSC_pivoted['Hit_z_mean'][i].loc[(event, lumi, muon)]
                    Muon_CSC_x_std = mergedCSC_pivoted['Hit_x_std'][i].loc[(event, lumi, muon)]
                    Muon_CSC_y_std = mergedCSC_pivoted['Hit_y_std'][i].loc[(event, lumi, muon)]
                    Muon_CSC_z_std = mergedCSC_pivoted['Hit_z_std'][i].loc[(event, lumi, muon)]
                    Muon_CSC_x_skew = mergedCSC_pivoted['Hit_x_skew'][i].loc[(event, lumi, muon)]
                    Muon_CSC_y_skew = mergedCSC_pivoted['Hit_y_skew'][i].loc[(event, lumi, muon)]
                    Muon_CSC_z_skew = mergedCSC_pivoted['Hit_z_skew'][i].loc[(event, lumi, muon)]
                    Muon_CSC_x_kurt = mergedCSC_pivoted['Hit_x_kurt'][i].loc[(event, lumi, muon)]
                    Muon_CSC_y_kurt = mergedCSC_pivoted['Hit_y_kurt'][i].loc[(event, lumi, muon)]
                    Muon_CSC_z_kurt = mergedCSC_pivoted['Hit_z_kurt'][i].loc[(event, lumi, muon)]
                    if i == 1:
                        Muon_CSC_s1 = Muon_CSC_s1.append(pd.DataFrame([[event,lumi,muon,Muon_CSC_nhits,Muon_CSC_x_mean,Muon_CSC_y_mean,Muon_CSC_z_mean,Muon_CSC_x_std,Muon_CSC_y_std,Muon_CSC_z_std,Muon_CSC_x_skew,Muon_CSC_y_skew,Muon_CSC_z_skew,Muon_CSC_x_kurt,Muon_CSC_y_kurt,Muon_CSC_z_kurt]], columns=Muon_CSC_s1.columns))
                    elif i == 2:
                        Muon_CSC_s2 = Muon_CSC_s2.append(pd.DataFrame([[event,lumi,muon,Muon_CSC_nhits,Muon_CSC_x_mean,Muon_CSC_y_mean,Muon_CSC_z_mean,Muon_CSC_x_std,Muon_CSC_y_std,Muon_CSC_z_std,Muon_CSC_x_skew,Muon_CSC_y_skew,Muon_CSC_z_skew,Muon_CSC_x_kurt,Muon_CSC_y_kurt,Muon_CSC_z_kurt]], columns=Muon_CSC_s2.columns))
                    elif i == 3:
                        Muon_CSC_s3 = Muon_CSC_s3.append(pd.DataFrame([[event,lumi,muon,Muon_CSC_nhits,Muon_CSC_x_mean,Muon_CSC_y_mean,Muon_CSC_z_mean,Muon_CSC_x_std,Muon_CSC_y_std,Muon_CSC_z_std,Muon_CSC_x_skew,Muon_CSC_y_skew,Muon_CSC_z_skew,Muon_CSC_x_kurt,Muon_CSC_y_kurt,Muon_CSC_z_kurt]], columns=Muon_CSC_s3.columns))
                    elif i == 4:
                        Muon_CSC_s4 = Muon_CSC_s4.append(pd.DataFrame([[event,lumi,muon,Muon_CSC_nhits,Muon_CSC_x_mean,Muon_CSC_y_mean,Muon_CSC_z_mean,Muon_CSC_x_std,Muon_CSC_y_std,Muon_CSC_z_std,Muon_CSC_x_skew,Muon_CSC_y_skew,Muon_CSC_z_skew,Muon_CSC_x_kurt,Muon_CSC_y_kurt,Muon_CSC_z_kurt]], columns=Muon_CSC_s4.columns))

     

    # MERGE THE DATAFRAMES AND STORE THE OUTPUT

    output = Muon_DT_s1.merge(Muon_DT_s2,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_DT_s3,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_DT_s4,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_CSC_s1,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_CSC_s2,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_CSC_s3,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid']).merge(Muon_CSC_s4,on=['Muon_Eventid','Muon_EventluminosityBlock','Muon_Muonid'])
    output.to_csv('STEP1/output' + args.FileNumber + '.csv', index=False)

