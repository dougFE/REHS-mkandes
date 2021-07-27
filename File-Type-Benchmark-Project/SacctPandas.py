import os
from oasis.scratch.comet.dougfe.temp_project.pandasSacct.pandas import pandas as pd
from oasis.scratch.comet.dougfe.temp_project.pandasSacct.pandas-sacct import sacct

inputPath = (open(os.path.abspath('testIDs'))).readlines()
ids = ''.join(inputPath)
ids = ids.replace('\n',',')
ids = ids.replace(' ','')

compressed_df = 'sacctPandasDF.pickle'

#This line queries sacct with all these parameters TODO: look at using --long, which includes all formatting in one command
s = sacct.Sacct(jobs=ids, noheader=True, parsable2=True, X=True, L=True, format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'])
df = s.execute()
print(s)
print(df.head())
