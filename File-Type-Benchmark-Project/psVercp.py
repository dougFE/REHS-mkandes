import sacct
import pandas as pd
t0 = pd.Timestamp("2020-08-30T00:00:00")
t1 = t0 + pd.Timedelta(days=1)
s = sacct.Sacct(starttime="2020-12-11T00:00:00", endtime="2020-12-11T00:30:00", format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'], allusers=True)
df = s.execute()
print(df)

