import multiprocessing
import os
import subprocess

# Function queryC takes a list of jobids, automatically queries comet and returns sacct data in a consistent format.
def queryC (idLs):

    newOut = open('sacctData_%s-%s.txt' % ((idLs[0])[0:8], (idLs[-1])[0:8]), 'w')
    tempOut = []
    for jobid in idLs:
        idOut = subprocess.check_output(["sacct", "-j", str(jobid), "--noheader", "--parsable2", "-X", "--format=JobIDRaw,JobID,UID,GID,AssocID,Cluster,JobName,User,Group,Account,Partition,QOS,QOSRAW,NNODES,NCPUS,AllocCPUS,ReqCPUS,ReqCPUFreq,ReqMem,Timelimit,Priority,State,ExitCode,DerivedExitCode,Submit,Eligible,Start,End,Time,Elapsed,Reserved,ResvCPU,ResvCPURaw,CPUTime,Comment,NodeList"])
        tempOut.append(idOut.decode('utf-8'))
        if len(tempOut) >= 2 or jobid == idLs[-1]:
            for data in tempOut:
                newOut.write(data)
            tempOut = []
    print('Output file complete: %s' % ((newOut.name).replace('\n', '')))
    newOut.close()

inputPath = open(os.path.abspath('testIDs'), 'r') # Location of file containing IDs.

outS = 5  # Desired size of output file (measured by # of jobids included in query).
mainLs = [] # mainLs used to store (outS)-length lists of jobids for the code to format later.
currentOut = []

""" This iterates through each line with a new temporary output list (currentOut) and when 5  items are added to
currentOut it's saved to mainLs and wiped clean for the next 5 jobids."""
for line in inputPath:
    if outS == 0:
        mainLs.append(currentOut)
        currentOut = []
        outS = 5
    currentOut.append(line)
    outS -= 1
if outS != 0:
    mainLs.append(currentOut) # This just prints out extra values if the temporary currentOut doesn't stop at 5 items.
print (mainLs)

# This runs parallel processes for each output file necessary. (They use individual memory partitions)
if __name__ == '__main__':
    for chunk in mainLs:
        p = multiprocessing.Process(target=queryC, args=(chunk,))
        p.start()
        print('Parallel process initialized. . . ')
