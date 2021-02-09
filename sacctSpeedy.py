
import os
import subprocess
import multiprocessing

jobIDList = [] #will be the main list of list of jobIDs
interval = 1000 #set whatever interval

with open("/home/caolinnh/SLURM_JOB_IDs-feb-2019.txt", "r") as jobFile:  
    jobIDs = jobFile.read().splitlines() #just a list of all jobIDs

lineNum = len(jobIDs)

memory = list() #will become a list of the sacct outputs to reduce time opening and closing files
memLim = 1000 #must be <= interval, and a perfect multiple

i = 0

while i < 10000:   #populates the jobIDList correctly according to the interval
    if ((i % interval) == 0):
        jobIDList.append(list())
        newFile = open(str(jobIDs[i]) + "-I-" + str(interval) + ".try", "a")
        newFile.close()
    jobIDList[int(i / interval)].append(jobIDs[i])
    i = i + 1

def query (idLs):  #actaully gets the data and puts it into files
    for he in idLs:  
        notPretty = subprocess.check_output(["sacct", "-j", str(he), "--noheader","--parsable2","-X","--format=JobIDRaw,JobID,UID,GID,AssocID,Cluster,JobName,User,Group,Account,Partition,QOS,QOSRAW,NNODES,NCPUS,AllocCPUS,ReqCPUS,ReqCPUFreq,ReqMem,Timelimit,Priority,State,ExitCode,DerivedExitCode,Submit,Eligible,Start,End,Time,Elapsed,Reserved,ResvCPU,ResvCPURaw,CPUTime,Comment,NodeList"])
        memory.append(notPretty.decode("utf-8")) #originally in bytes so decodes it to string
        if len(memory) >= memLim:  #if it reaches the memory limit originally set it will dump it into proper file
            thisFile = open(str(jobIDList[jobIDList.index(idLs)][0]) + "-I-"+ + str(interval) + ".try", "a")
            for pls in memory:
                            thisFile.write(pls)
            thisFile.close()
            memory.clear()

if __name__ ==  '__main__':
    for part in jobIDList: #parallelizes it by assigning lists to different cores
        p = multiprocessing.Process(target = query, args=(part , ))
        p.start()
