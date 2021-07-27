import sacct
import pandas as pd
import os

t0 = pd.Timestamp("2020-06-12T16:00:00")  # Start date of query
t1 = t0 + pd.Timedelta(seconds=1)  # Time interval (after start) of query. Can use days, seconds, minutes, hours, weeks, etc

df_filename = 'sacctData2.csv'  # Name of desired or existing compressed sacct dataframe
df_path = ''  # Desired or existing relative path to ^same^ df


# Query comet with sacct for all jobs running between t0 and t1, turn info into pandas df
s = sacct.Sacct(starttime=t0, endtime=t1, format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'], allusers=True)
df = s.execute()
df.to_csv(df_path+df_filename)
