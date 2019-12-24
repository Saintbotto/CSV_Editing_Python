
import pickle
import csv
import re
import os
import os.path
from multiprocessing import Pool, TimeoutError, Process,current_process
from variables_V2 import *
import pandas as pd
import json
import itertools

from pandas import DataFrame

write_count = 0
                    
#makes 'final_df'


#function to create 'clean_df'. this is equal to 'master_df minus' thumbnail data  
def make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path):
    # seperates file names from file path cell
    def make_final_df(fileNameList, clean_df, sql, output, output_path, output_filename, input_path):
        global write_count
        # with open('variables.pickle', 'rb') as f:
        # sql, output, output_path, output_filename, input_path = pickle.load(f)
        # f.close()
        final_df = pd.DataFrame()
        filename_df = pd.DataFrame()
        filename_df = pd.DataFrame(fileNameList)
        filename_df.transpose()
        filename_df.columns = ['filename']
        final_df = clean_df.assign(filename=filename_df['filename'].values)
        # checks if the '--SQL' arguement was used and writes final_df to the appropriate location
        # if sql == False:
        #     # create and append the specified output file
        #     with open(output_path, 'a', newline='') as csvfile:
        #         if (write_count == 0):
        #             write_count = write_count + 1
        #             final_df.to_csv(output_path, index=False)
        #         else:
        #             final_df.to_csv(output_path, mode='a', index=False, header=False)
        # # if '--SQL' was used,call the function 'write_database' and pass 'final_df'
        # elif sql == True:
        #     output_path_dbcsv = output_path + ".csv"
        #     with open(output_path_dbcsv, 'a', newline='') as csvfile:
        #         if (write_count == 0):
        #             write_count = write_count + 1
        #             final_df.to_csv(output_path_dbcsv, index=False)
        #         else:
        #             final_df.to_csv(output_path_dbcsv, mode='a', index=False, header=False)
        # else:
        #     print("error")
        return final_df
        final_df.drop(columns=column_list_appended)
        filename_df.drop(columns='filename')

    def filename_seperator(clean_df, sql, output, output_path, output_filename, input_path):
        filename = ""
        filePathList = []
        filePathList = []
        fileNameList = []
        splitPath = []
        filePathList = clean_df['filepath'].astype(str).values.tolist()
        # for every value in the list, split it, and append the last entry to a new list
        finalDFlist=[]
        for i in filePathList:
            filepath = i
            splitPath = filepath.split('/')
            filename = splitPath[-1]
            fileNameList.append(filename)
            # call the function that makes 'final_df' and pass necessecary variables
            finalDFlist.append(make_final_df(fileNameList, clean_df, sql, output, output_path, output_filename, input_path))


        filePathList.clear()
        fileNameList.clear()
        splitPath.clear()
        return finalDFlist
        del filename

    clean_df = pd.DataFrame()
    extra_df = pd.DataFrame()
    extra_df = pd.DataFrame(lighterExtra)
    extra_df.transpose()
    extra_df.columns = ['extra']
    #create 'clean_df' and assign it the values of master_df and extra_df
    clean_df = master_df.assign(extra=extra_df['extra'].values)
    finalDFlist=filename_seperator(clean_df, sql, output, output_path, output_filename, input_path)

    clean_df.drop(columns=column_list_altered)
    extra_df.drop(columns='extra')
    return finalDFlist

def thumbnail_remover(master_df, sql, output, output_path, output_filename, input_path):
    fullExtraList = []
    lighterExtra = []
    splitExtra = []
    master_df.dropna()
    
    fullExtraList = master_df['extra'].astype(str).values.tolist()
    master_df.drop(columns = 'extra')
    for i in fullExtraList:
        #do the below commands if the cell does not return 'NaN' (not a number)
        if i !='nan':
            extra = i
            #check to see if the 'extra' column of the csv contains thumbnail data
            extra_test = '; thumbnail: ' in extra
            #empty_test = '' in extra
            #if the cell contains data, do the do the following
            if (extra_test == True):
                #split the string at '; thumbnail: '
                splitExtra = extra.split('; thumbnail: ')
                #add the first string to lighter.extra... thumbnail data is always at the end
                lighterExtra.append(splitExtra[0])
                splitExtra.clear()
                #call the function 'male_clean_df'
                finalDFlist=make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path)
            #if the string does not contain thumbnail data, append it to lighterExtra
            elif (extra_test == False):
                lighterExtra.append(extra)
                #call the function 'make_clean_df'
                finalDFlist=make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path)
            #handling unexpected issues. file output is weird and this is prettyer than a debug diag
            else:
                print("unknown Issue parsing Extra column")
                kill_switch = True
                #updayes 'kill.pickles' sp exit the code
                with open('kill.pickle', 'wb') as f:
                    pickle.dump([kill_switch], f)
        #if the input is not valid or detected as a specific invalid type, skip it
        else:
            return()
        #clear variables and lists for reuse. lighten memory usage
        del extra
        lighterExtra.clear()
        fullExtraList.clear()
        return finalDFlist

def date_cleaner(master_df, sql, output, output_path, output_filename, input_path):
    fullDateList = []
    fullDateList = master_df['date'].astype(str).values.tolist()
    regex = re.compile('[@!#$%^&*()<>?\|}{~;":_-]')
    for i in fullDateList:
        date = i
        #print(regex.search(date))
        if (regex.search(date) == None):
            fullDateList.clear()
            finalDFlist=thumbnail_remover(master_df, sql, output, output_path, output_filename, input_path)
        elif (regex.search(date) != None):
            fullDateList.clear()
            #del fullExtraList[:]
            return()           
        else:
            print("unknown Issue parsing Extra column")
            kill_switch = True
            with open('kill.pickle', 'wb') as f:
                pickle.dump([kill_switch], f)
        del date
    return finalDFlist
def jerb(chunk):
    """
    with open('variables.pickle', 'rb') as f:
    sql, output, output_path, output_filename, input_path = pickle.load(f)
    f.close()

    :param chunk:
    :return:
    """
    with open('variables.txt', 'r') as f:
        data=json.load(f)
        sql=data['sql']
        output=data['output']
        output_path=data['output_path']
        output_filename=data['output_filename']
        input_path = data['input_path']
        f.close()

    master_df = pd.DataFrame()
    master_df = pd.concat([master_df, chunk])
    master_df.rename(columns=({'filename': 'filepath'}), inplace=True)
    finalDFlist=date_cleaner(master_df, sql, output, output_path, output_filename, input_path)
    master_df.drop(columns=column_list_altered)
    return finalDFlist

def test(x):
    print(x)
    return x
def read():
    with open('variables.txt', 'r') as f:
        data=json.load(f)
        sql=data['sql']
        output=data['output']
        output_path=data['output_path']
        output_filename=data['output_filename']
        input_path = data['input_path']
        f.close()
    final_dfArray = []
    if current_process().name=='MainProcess':
        print('Reading Input into Memory')
        with open(input_path) as PF:
            chunk_iter = pd.read_csv(PF, usecols=column_list, chunksize = 1)
            chunk_array=[]
            for i in chunk_iter:
                chunk_array.append(i)

            # Moved to jerb
            #for chunk in chunk_iter:
            #     master_df = pd.DataFrame()
            #     master_df = pd.concat([master_df, chunk])
            #     master_df.rename(columns=({'filename': 'filepath'}), inplace=True)
            #     date_cleaner(master_df, sql, output, output_path, output_filename, input_path, final_dfArray)
            #     master_df.drop(columns=column_list_altered)

    """
    Multiprocessing Chunk
    """
    print('Spinning up Pools')
    pool = Pool(processes=10)
    dtf_array=pool.map(jerb, chunk_array)
    pool.close()
    pool.close()
    pool.join()
    """
    Serial Chunk
    """
    # for chunk in chunk_array:
    #     final_dfArray.append(jerb(chunk))
    print('Outputting')
    with open(output_path,'w+') as o:
        o.truncate()
        o.close()
    final_dfArray=[]
    for dtf in dtf_array:
        if type(dtf)==list:
            for v in dtf:
                final_dfArray.append(v)
    final_dfArray=pd.concat(final_dfArray)
    final_dfArray.to_csv(output_path, mode='a', index=False, header=False)


