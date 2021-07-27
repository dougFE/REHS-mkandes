import sys
import os
import pandas as pd
import sacct

#ids = ['20122150','20122152','20122153','20122644','20122645','20122647']
#ids = ''.join(inputPath)
#ids = ids.replace('\n',',')
#ids = ids.replace(' ','')

#compressed_df = 'sacctPandasDF.pickle'

#This line queries sacct with all these parameters TODO: look at using --long, which includes all formatting in one command
s = sacct.Sacct(jobs='20122150', noheader=True, parsable2=True, format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'])
df = s.execute()
print(df)
