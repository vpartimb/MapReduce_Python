import pandas as pd
import subprocess as sp
import multiprocessing as mp
import time
from functools import partial
import os
from os import listdir
from os.path import isfile, join
import random

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
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("    Machine " + machine + " is down")
        pass
    # If command returns something
    else:
        # If an error message is returned
        if myProcess.returncode:
            print("    Problem with machine " + machine)
        # If all went well
        else:
            return machine
    pass

def runSlave(machine, option, file=None):
    """
    Runs the SLAVE program on 'machine'
    """
    try:
        # Trying to run SLAVE
        if file == None:
            cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' python3 /tmp/vpartimbene/SLAVE.py ' + option
            myProcess = sp.run(cmd, shell=True, capture_output=True)
        else:
            cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' python3 /tmp/vpartimbene/SLAVE.py ' + option + ' ' + file
            myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=600)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("    Machine " + machine + " is down")
        pass
    # If command returns something
    else:
        # If an error message is returned
        if myProcess.returncode:
            print(myProcess.stdout.decode("utf-8").rstrip())
        # If all went well
        else:
            pass
    pass

def copySplitToMachine(split, machine):
    """
    Copies 'split' to 'machine'
    """
    try:
        # Trying directory creation on the remote machine
        cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' mkdir -p /tmp/vpartimbene/splits'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
        # Trying to copy file
        cmd = 'scp -r -p /tmp/vpartimbene/splits/' + split + ' vpartimbene@' + machine + ':/tmp/vpartimbene/splits/'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("    Machine " + machine + " is down")
        pass
    # If command returns something
    else:
        # If an error message is returned
        if myProcess.returncode:
            print("    Copy problem with machine " + machine)
            print(myProcess.stderr)
        # If all went well
        else:
            print("    Split " + split + " copied to " + machine)
    pass

def copyFileToMachine(file, machine):
    """
    Copies 'file' on 'machine'
    """
    try:
        # Trying directory creation on the remote machine
        cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + machine + ' mkdir -p /tmp/vpartimbene'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
        # Trying to copy file
        cmd = 'scp -r -p ' + file + ' vpartimbene@' + machine + ':/tmp/vpartimbene/'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("    Machine " + machine + " is down")
        pass
    # If command returns something
    else:
        # If an error message is returned
        if myProcess.returncode:
            print("    Copy problem with machine " + machine)
            print(myProcess.stderr)
        # If all went well
        else:
            print("    File " + file + " copied to " + machine)
    pass

def readAndSplit(file, nb):
    """
    Reads the input 'file' and splits it in 'nb' splits
    """
    try:
        # Trying directory creation on the local machine
        cmd = 'mkdir -p /tmp/vpartimbene/splits/'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
        # Trying to create splits in directory
        cmd = f'split -d -nl/{nb} -a2 --additional-suffix=.txt {file} /tmp/vpartimbene/splits/S'
        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=50)
    # If timeout is reached
    except sp.TimeoutExpired:
        print("Machine " + machine + " is down")
        pass
    # If command returns something
    else:
        # If an error message is returned
        if myProcess.returncode:
            print("Error: unknown Unix command")
            print(myProcess.stderr)
        # If all went well
        else:
            print("Split of " + file + " in " + str(nb) + " splits")
    pass

if __name__ == '__main__':
    # Machine name retrieval
    machines = openFile("Machines_TP.csv")

    # Machine status check (in parallel)
    print("Checking machines ...")
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

    # Split of input file
    readAndSplit('deontologie_police_nationale.txt', 4)
    print('')

    splits = [f for f in listdir("/tmp/vpartimbene/splits") if isfile(join("/tmp/vpartimbene/splits", f))]
    usedMachines = random.sample(runningMachines, len(splits))

    with open('/tmp/vpartimbene/machine.txt', 'w') as f:
        f.write('\n'.join(usedMachines))
    
    # Copy of 'machine.txt' on used machines
    print("COPYING 'machine.txt'")
    print('')
    function = partial(copyFileToMachine, '/tmp/vpartimbene/machine.txt')
    with mp.Pool(6) as p:
        p.map(function, usedMachines)
    print('')
    print("COPY DONE", end="\n\n")

    # Copy of splits on running machines
    print("SPLIT STARTED")
    print('')
    with mp.Pool(6) as p:
        p.starmap(copySplitToMachine, zip(splits, usedMachines))
    print('')
    print("SPLIT FINISHED", end="\n\n")

    # Running SLAVE.py with option '0' to map
    startMap = time.time()
    print('MAP STARTED')
    with mp.Pool(6) as p:
        p.starmap(runSlave, zip(usedMachines, ['0'] * len(usedMachines), splits))
    print('...')
    print("MAP FINISHED")
    print("+++ MAP duration: " + str(time.time() - startMap) + " s +++", end="\n\n")

    # Running SLAVE.py with option '1' to shuffle
    startShuff = time.time()
    print('SHUFFLE STARTED')
    maps = [f'UM{splits[i][1:3]}.txt' for i in range(len(usedMachines))]
    with mp.Pool(6) as p:
        p.starmap(runSlave, zip(usedMachines, ['1'] * len(usedMachines), maps))
    print('...')
    print("SHUFFLE FINISHED")
    print("+++ SHUFFLE duration: " + str(time.time() - startShuff) + " s +++", end="\n\n")

    # Running SLAVE.py with option '2' to reduce
    startRed = time.time()
    print('REDUCE STARTED')
    with mp.Pool(6) as p:
        p.starmap(runSlave, zip(usedMachines, ['2'] * len(usedMachines)))
    print('...')
    print("REDUCE FINISHED")
    print("+++ REDUCE duration: " + str(time.time() - startRed) + " s +++", end="\n\n")