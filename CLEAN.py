import pandas as pd
import subprocess as sp
import multiprocessing as mp
import time
from functools import partial
import os

# os.chdir(r"C:\Users\vince\Google Drive\MS BGD\Cours\INF727_Systemes_repartis_pour_Big_Data\TP")

def openFile(file):
    """
    Returns the list of machines in 'file'
    """
    machinesCSV = pd.read_csv("Machines_TP.csv")
    return machinesCSV["Machines"].values.tolist()

def status(machine):
    """
    Checks if 'machine' is up
    """
    # System command line
    cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' hostname'
    try:
        # Command line execution
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
    # If timeout is reached
    except sp.TimeoutExpired:
        #print(machine + " down")
        pass
    # If command line returns something
    else:
        # If all went well
        if not myProcess.returncode:
            return machine
        # If an error is raised
        else:
            #print("Problem with machine " + machine)
            pass

#Fonction qui copie un fichier sur une machine
def cleanMachine(directory, machine):
    """
    Removes 'directory' from 'machine'
    """
    try:
        # Trying directory removal on the remote machine
        cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' rm -rf ' + directory
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("    " +  machine + " is down")
        pass
    # If command line returns something
    else:
        # If an error is raised
        if myProcess.returncode:
            print("    Cleaning problem with machine " + machine)
            print(myProcess.stderr)
        # If all went well
        else:
            print("    " + machine + " has been cleaned")
        pass

if __name__ == '__main__':
    # Machine name retrieval
    machines = openFile("Machines_TP.csv")

    # Machine status check (in parallel)
    machinesTemp = []
    start = time.time()
    with mp.Pool(6) as p:
        machinesTemp = p.map(status, machines)

    # Only running machines are kept
    runningMachines = []
    for val in machinesTemp:
        if val != None:
            runningMachines.append(val)
    print("Number of running machines: " + str(len(runningMachines)))
    print("Found in " + str(time.time() - start) + " s", end="\n\n")

    # Rmoval of repertory /tmp/vpartimbene on all running machines (in parallel)
    print("CLEAN STARTED ...")
    print('')
    directory = '/tmp/vpartimbene/'
    # Using a partial function because map can only take 2 arguments
    fonction = partial(cleanMachine, directory)
    with mp.Pool(6) as p:
        p.map(fonction, runningMachines)
    print('')
    print("... CLEAN FINISHED", end="\n\n")

