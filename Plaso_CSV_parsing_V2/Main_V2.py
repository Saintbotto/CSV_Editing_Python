import pickle
import time
import os
import os.path
import ArgParse_V1
import VerifyPaths_V2
import cleanCSV_V2
import databaseMaker_V1
import time
import sys
import multiprocessing
import variablesTextTest
def kill_switch():
    if os.path.isfile('kill.pickle') == True:
        with open('kill.pickle', 'rb') as f:
            kill_switch = pickle.load(f)
        if kill_switch == True:
            os.remove(database_variables.pickle)
            os.remove(kill.pickle)
            exit()
        else:
            return()
    else:
        return()

def sql_check():
    with open('variables.pickle', 'rb') as f:
        sql, output, output_path, output_filename, input_path = pickle.load(f)
    if sql == True:
        print("Making Database")
        databaseMaker_V1.write()
        print("Database Built")
    elif sql == False:
        exit()
    else:
        exit()

def startgraphic():
    print("    ____  __                    ____________    __   ______    ___ __     ")
    print("   / __ \/ /___ __________     / ____/ ___/ |  / /  / ____/___/ (_) /_____  _____")
    print("  / /_/ / / __ `/ ___/ __ \   / /    \__ \| | / /  / __/ / __  / / __/ __ \/ ___/")
    print(" / ____/ / /_/ (__  ) /_/ /  / /___ ___/ /| |/ /  / /___/ /_/ / / /_/ /_/ / /    ")
    print("/_/   /_/\__,_/____/\____/   \____//____/ |___/  /_____/\__,_/_/\__/\____/_/     ")
    print("Because the size of your data really does matter")



if __name__ == "__main__":
    start_time = time.time()
    startgraphic()
    time.sleep(3)
    print(multiprocessing.current_process().name)
    if multiprocessing.current_process().name=='MainProcess':
        variablesTextTest.parse()
    kill_switch()
    cleanCSV_V2.read()
    print("--- {} seconds ---".format(time.time() - start_time))
    kill_switch()
    #sql_check()




