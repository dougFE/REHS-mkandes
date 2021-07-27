import sacct
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq

t0 = pd.Timestamp("2020-09-29T00:00:00")  # Start date of query
t1 = t0 + pd.Timedelta(minutes=5)  # Time interval (after start) of query. Can use days, seconds, minutes, hours, weeks, etc

df_filename = 'df_sacct.parquet'  # Name of desired or existing compressed sacct dataframe
df_path = ''  # Desired or existing relative path to ^same^ df


# Query comet with sacct for all jobs running between t0 and t1, turn info into pandas df
s = sacct.Sacct(starttime=t0, endtime=t1, format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'], allusers=True)
df = s.execute()
print(df)

# Put df into pyarrow
table = pa.Table.from_pandas(df, preserve_index=True)

if not os.path.exists(df_path + df_filename):
    print('Main sacct dataframe nonexistent. Making file: ' + df_filename)
    pq.write_table(table, df_path+df_filename)

else:
    print('Main sacct dataframe already exists. Writing query to parquet')
    pq.write_to_dataset(table,root_path=(df_path+df_filename))
