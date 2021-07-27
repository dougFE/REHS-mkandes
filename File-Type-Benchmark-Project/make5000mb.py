import random
import os

sourceDF = 'source(240k).csv'
targetsize = 5000000000
newName = '5000mb.csv'
newDF = open(newName, 'w')
newDF.write(',JobIDRaw,step,JobID,UID,GID,AssocID,Cluster,JobName,User,Group,Account,Reservation,ReservationId,Partition,QOS,QOSRAW,NNODES,NTASKS,NCPUS,AllocCPUS,ReqCPUS,ReqCPUFreq,ReqMem,ReqGRES,AllocGRES,Timelimit,Priority,State,ExitCode,DerivedExitCode,Submit,Eligible,Start,End,Time,Elapsed,Reserved,Suspended,AveCPU,MinCPU,MinCPUNode,MinCPUTask,ResvCPU,ResvCPURaw,TotalCPU,SystemCPU,UserCPU,CPUTime,CPUTimeRaw,AveCPUFreq,AveDiskRead,MaxDiskRead,MaxDiskReadNode,MaxDiskReadTask,AveDiskWrite,MaxDiskWrite,MaxDiskWriteNode,MaxDiskWriteTask,AvePages,MaxPages,MaxPagesNode,MaxPagesTask,AveRss,MaxRSS,MaxRSSNode,MaxRSSTask,AveVMSize,MaxVMSize,MaxVMSizeNode,MaxVMSizeTask,ConsumedEnergy,ConsumedEnergyRaw,Layout,Comment,NodeList\n')

sourceDF = open(sourceDF, 'r')
sourceLines = sourceDF.readlines()
sourceDF.close()


"""
Main Loop:
if filesize < goal:
    randomly pick line from original
    write that line to the new bigger dataset

"""

fileSize = 0
sourceSize = len(sourceLines)-5

# New line to write header
newDF.write(sourceLines[0])

while fileSize < targetsize:
    randLine = sourceLines[random.randrange(sourceSize)+1]
    newDF.write(randLine)
    fileSize = os.path.getsize(newName)
