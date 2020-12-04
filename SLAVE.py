#!/usr/bin/env python3
import time
import sys
import subprocess as sp
import socket
import zlib
import os

machine = socket.gethostname()

def main():
    if len(sys.argv) == 3:
        option = sys.argv[1]
        filename = sys.argv[2]

        #############
        #    MAP    #
        #############
        if option == '0':
            try:
                # Trying directory creation on the machine
                cmd = 'mkdir -p /tmp/vpartimbene/maps'
                myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
                # Trying UMx.txt file creation on the machine
                cmd = f'touch /tmp/vpartimbene/maps/UM{filename[1:3]}.txt'
                myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
            except sp.CalledProcessError:
                print(f'Mapping on ' + machine + ' failed')

            with open(f'/tmp/vpartimbene/splits/{filename}', 'r', encoding="utf8") as f:
                listWords = []
                for line in f:
                    listWords += line.split()

            for word in listWords:
                word = f'{word} 1'
                try:
                    # Trying to fill UMx
                    with open(f'/tmp/vpartimbene/maps/UM{filename[1:3]}.txt', 'a') as f:
                        f.write(word + '\n')
                except sp.CalledProcessError:
                    print(f'Mapping on ' + machine + ' failed')

    
        #################
        #    SHUFFLE    #
        #################
        elif option == '1':
            with open(f'/tmp/vpartimbene/maps/{filename}', 'r', encoding="utf8") as f:
                lines = f.read().splitlines()

            listWords = []
            for line in lines:
                listWords.append(line.strip("1,' '"))

            listHash = []
            for word in listWords:
                listHash.append(zlib.adler32(bytearray(word, "utf8")))
            listUniqueHash = set(listHash)
            try:
                # Trying directory creation on the machine
                cmd = 'mkdir -p /tmp/vpartimbene/shuffles'
                myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=10)
                # Trying <hash>-<hostname>.txt file creation on the machine
                for line, wordHash in zip(lines, listHash):
                    with open(f'/tmp/vpartimbene/shuffles/{wordHash}-{machine}.txt', 'a') as f:
                        f.write(line + '\n')
            except sp.CalledProcessError:
                print(f'Shuffling on ' + machine + ' failed')

            with open(f'/tmp/vpartimbene/machine.txt', 'r', encoding="utf8") as f:
                listMachines = f.read().splitlines()
            for wordHash in list(listUniqueHash):
                machineIndex = wordHash % len(listMachines)
                if listMachines[machineIndex] != machine:
                    try:
                        # Trying directory creation on the remote machine
                        cmd = 'ssh -o "StrictHostKeyChecking=no" vpartimbene@' + listMachines[machineIndex] + ' mkdir -p /tmp/vpartimbene/shufflesreceived'
                        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
                        # Trying to copy file
                        cmd = f'scp -r -p /tmp/vpartimbene/shuffles/{wordHash}-{machine}.txt vpartimbene@' + listMachines[machineIndex] + ':/tmp/vpartimbene/shufflesreceived/'
                        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=1000)
                    except sp.CalledProcessError:
                        print(f'Shuffling on ' + machine + ' failed')
                else:
                    try:
                        # Trying directory creation on the local machine
                        cmd = 'mkdir -p /tmp/vpartimbene/shufflesreceived'
                        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
                        # Trying to copy file
                        cmd = f'cp /tmp/vpartimbene/shuffles/{wordHash}-{machine}.txt /tmp/vpartimbene/shufflesreceived/{wordHash}-{machine}.txt'
                        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
                    except sp.CalledProcessError:
                        print(f'Shuffling on ' + machine + ' failed')

        else:
            print ('Unknown option: ' + option + '  --Options are: 0 (Map) or 1 (Shuffle) or 2 (Reduce)')
            sys.exit(1)
            

    elif len(sys.argv) == 2:
        option = sys.argv[1]
        ################
        #    REDUCE    #
        ################
        if option == '2':
            if os.path.isdir('/tmp/vpartimbene/shufflesreceived'):
                try:
                    # Trying directory creation on the local machine
                    cmd = 'mkdir -p /tmp/vpartimbene/reduces'
                    myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
                    listHash = [f.split('-')[0] for f in os.listdir('/tmp/vpartimbene/shufflesreceived')]
                    listUniqueHash = list(set(listHash))
                    for uniqueHash in listUniqueHash:
                        cmd = f'cat /tmp/vpartimbene/shufflesreceived/{uniqueHash}* >> /tmp/vpartimbene/reduces/{uniqueHash}.txt'
                        myProcess = sp.run(cmd, shell=True, capture_output=True, timeout=20)
                        with open(f'/tmp/vpartimbene/reduces/{uniqueHash}.txt', 'r', encoding="utf8") as f:
                            lines = f.read().splitlines()
                            word = lines[0].split()[0]
                            wordCount = len(lines)
                        with open(f'/tmp/vpartimbene/reduces/{uniqueHash}.txt', 'w', encoding="utf8") as f:
                            f.write(f'{word} {wordCount}')
                except sp.CalledProcessError:
                    print(f'Reducing on ' + machine + ' failed')

        else:
            print ('Unknown option: ' + option + '  --Options are: 0 (Map) or 1 (Shuffle) or 2 (Reduce)')
            sys.exit(1)

    else:
        print ('Usage: ./SLAVE.py {0 | 1 | 2} filename --> filename: optional')
        sys.exit(1)

if __name__ == '__main__':
    main()