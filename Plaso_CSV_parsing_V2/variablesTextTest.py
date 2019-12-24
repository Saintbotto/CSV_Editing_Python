import argparse
import pickle
import os
import os.path
import json


def update_pickle(sql, output, output_path, output_filename, input_path):
    print(sql, output, output_path, output_filename, input_path)

    # create/open variables.pickleand write to it in bytes
    with open('variables.pickle', 'wb') as f:
        # store the specified variables in 'variables.pickle'
        pickle.dump([sql, output, output_path, output_filename, input_path], f)
        f.close()
        # close the file


# verifies all command arguments are valid
def verify(sql, output, output_file, input_path,returnDict):
    # check of output_file ends with '.db'
    db_test = output_file.endswith('.db')
    # check of output_file ends with '.csv'
    csv_test = output_file.endswith('.csv')
    returnDict={}
    if sql == True and db_test != True:
        # remove the incorrect file extension from the specified document
        output_filename, file_extension = os.path.splitext(output_file)
        # add the correct file type
        output_filename = output_filename + '.db'
        # print(output_filename)
        print("Renaming", output_file, "to", output_filename,
              "because it ended in... something other than it should have")
        output_path = output + '\\' + output_filename
        clean_csv = output_path + '.csv'
        # call the function 'update_pickle'
        update_pickle(sql, output, output_path, output_filename, input_path)
        returnDict = {'output_filename': output_filename,
                      'output_path': output_path,
                      'output': output}
    elif sql == False and csv_test != True:
        # remove the incorrect file extension from the specified document
        output_filename, file_extension = os.path.splitext(output_file)
        # add the correct file type
        output_filename = output_filename + '.csv'
        print("Renaming", output_file, "to", output_filename,
              "because it ended in... something other than it should have")
        output_path = output + '\\' + output_filename
        # call the function 'update_pickle'
        clean_csv = 'N/A'
        update_pickle(sql, output, output_path, output_filename, input_path)
        returnDict = {'output_filename': output_filename,
                      'output_path': output_path,
                      'output': output}
    elif (sql == True and db_test == True) or (sql == False and csv_test == True):
        # check if the file path given exists
        if os.path.exists(output) == True:
            output_filename = output_file
            # join_path(output, output_filename)
            output_path = output + '\\' + output_filename
            # call the function 'update_pickle'
            clean_csv = 'N/A'
            update_pickle(sql, output, output_path, output_filename, input_path)
            returnDict = {'output_filename': output_filename,
                          'output_path': output_path,
                          'output':output}
        else:
            # updates 'kill.pickle so the program can be exited
            print("File path for output file does not exist")
            print("Please try again")
            kill_switch = True
            with open('kill.pickle', 'wb') as f:
                pickle.dump([kill_switch], f)
    else:
        # updates 'kill.pickle so the program can be exited
        print("unknown exception in outputfilename")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
    return returnDict

def splitpath(dict):
    # splits output filename from its directory path so its existance can be verified
    returnDict={}
    variables=dict
    if variables['output_file'] == variables['input_path']:
        # updates 'kill.pickle so the program can be exited
        print("input and output file paths cannot be the same")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
    # check if the input file exists
    elif os.path.isfile(variables['input_path']) == True:
        # if so do the below commands
        output = variables['output_file'].split('\\')
        output_file = output[-1]
        del output[-1]
        output = "\\".join(output)
        returnDict=verify(variables['sql'], output, output_file, variables['input_path'],returnDict)
    # if not, updates 'kill.pickle so the program can be exited
    else:
        print("The input file path you have specified:", variables['input_path'], "does not exist")
        print("Please try again")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
    return returnDict


def parse():
    """
    The Following parse the Command Line arguments into Json which is then stored to a local file variables.txt
    """
    parser = argparse.ArgumentParser(description='set variables for csv parsing')
    parser.add_argument('--SQL', action="store_true", default=False, required=False,
    help="Output to SQL database, default is CSV")
    parser.add_argument('--o', action="store", dest="output_file", required=True,
    type=str, help="Specify output file path")
    parser.add_argument('--i', action="store", dest="input_path", required=True,
    type=str, help="Specify input file path")
    args = parser.parse_args()
    print(args)
    output_file = args.output_file
    input_path = args.input_path
    sql = args.SQL
    data = {
        'output_file': args.output_file,
        'input_path': args.input_path,
        'sql': args.SQL}
    fileparts=splitpath(data)
    data.update({
        'output':fileparts['output'],
        'output_path': fileparts['output_path'],
        'output_filename': fileparts['output_filename'],
    }
    )
    with open('variables.txt', 'w') as f:
        f.truncate()
        json.dump(data,f)

if __name__ == '__main__':
    parse()

