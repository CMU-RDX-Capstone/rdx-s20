# coding: utf-8

import pandas as pd

def extract(inputSet, outputfile, deviceList):
    """
    entry function of the extracting deveices info from raw data for further modeling
    :param inputSet: a list of input file (.csv)
    :param outputfile: the combined output(.csv) for all input
    :param deviceList: the device IDs list selected to do further modeling
    """
    final_df = pd.DataFrame()
    for inputfile in inputSet:
        df = extractDevice(inputfile, deviceList)
        final_df = final_df.append(df)
    final_df.to_csv(outputfile, encoding='utf-8', index=False)


def extractDevice(inputfile, deviceList):
    """
    extract the devices info from the input file
    :param inputfile: one input file
    :param deviceList: the device IDs list selected to do modeling
    :return: data frame to be combined later
    """
    # read 2M lines at a time, use four columns: poller_time,target_id,measured_value,metric_name
    chunks = pd.read_csv(inputfile, sep=',', usecols=[2, 3, 9, 13], chunksize=2048, header=0, error_bad_lines=False)
    # use the selected device list to do modeling
    df = pd.concat([chunk.loc[chunk['target_id'].isin(deviceList)] for chunk in chunks])
    return df

# input csv log files including path name
inputSet = ['D:/log data/original/2/event.log.20190909032250.csv',
          'D:/log data/original/2/event.log.20190910032651.csv',
          'D:/log data/original/2/event.log.20190911034326.csv',
          'D:/log data/original/2/event.log.20190912034837.csv',
          'D:/log data/original/2/event.log.20190913034449.csv',
          'D:/log data/original/2/event.log.20190914032912.csv',
          'D:/log data/original/2/event.log.20190915031653.csv',
          'D:/log data/original/2/event.log.20190916031900.csv']
# output path name
outputfile = 'combined.csv'
# selected device list to do modeling
deviceList = ['1640315','2421540','1833973','1478336']
extract(inputSet, outputfile, deviceList)











