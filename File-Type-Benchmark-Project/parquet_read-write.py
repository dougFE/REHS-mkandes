import sacct
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

beginTime = time.time()

t0 = pd.Timestamp("2020-06-14T00:00:00")  # Start date of query
t1 = t0 + pd.Timedelta(weeks=4)  # Time interval (after start) of query. Using a 1 week time period for benchmarking.

df_tryNum = input("\nInput attempt number here: ")  # Desired or existing relative path to ^same^
df_filename = df_tryNum + 'compressTest.parquet'  # Name of desired or existing compressed sacct dataframe


savetime = time.time()
print("\nBeginning sacct query, time is: " + str(time.time()))


# Query comet with sacct for all jobs running between t0 and t1, turn info into pandas df
s = sacct.Sacct(starttime=t0, endtime=t1, format=['JobIDRaw','JobID','UID','GID','AssocID','Cluster','JobName','User','Group','Account','Reservation','ReservationId','Partition','QOS','QOSRAW','NNODES','NTASKS','NCPUS','AllocCPUS','ReqCPUS','ReqCPUFreq','ReqMem','ReqGRES','AllocGRES','Timelimit','Priority','State','ExitCode','DerivedExitCode','Submit','Eligible','Start','End','Time','Elapsed','Reserved','Suspended','AveCPU','MinCPU','MinCPUNode','MinCPUTask','ResvCPU','ResvCPURaw','TotalCPU','SystemCPU','UserCPU','CPUTime','CPUTimeRaw','AveCPUFreq','AveDiskRead','MaxDiskRead','MaxDiskReadNode','MaxDiskReadTask','AveDiskWrite','MaxDiskWrite','MaxDiskWriteNode','MaxDiskWriteTask','AvePages','MaxPages','MaxPagesNode','MaxPagesTask','AveRss','MaxRSS','MaxRSSNode','MaxRSSTask','AveVMSize','MaxVMSize','MaxVMSizeNode','MaxVMSizeTask','ConsumedEnergy','ConsumedEnergyRaw','Layout','Comment','NodeList'], allusers=True)
print("\nQuery complete, df creation beginning. time is: " + str(time.time()) + ". Query runtime was: " + str(time.time() - savetime))
df = s.execute()


# Put df into pyarrow
print("\nPandas dataframe complete, writing to pyarrow table. Time is: " + str(time.time()))
savetime = time.time()
table = pa.Table.from_pandas(df, preserve_index=True)
convTime = time.time() # Time of conversion completion
print("\ndf converted to pyarrow table, beginning parquet compression and write. Conversion duration: " + str(time.time() - savetime))


# Write pyarrow table to parquet compressed file
print('Making file: ' + df_filename)
pq.write_table(table, df_filename)
print("\nCompression complete, file made. Compression+write from PA table time: " + str(time.time() - convTime))
print("Total compression time from pandas df to parquet file: " + str(time.time() - savetime))


# Now read the file from parquet for the final benchmark.
print("\npq file located, beginning read to pd dataframe. Time is: " + str(time.time()))
savetime = time.time()
df_read = pd.read_parquet(df_filename)
print("\nParquet read and loaded into pandas df. Load and read time: " + str(time.time() - savetime))
print("\n------------------------------------------\n Benchmark complete. Total runtime:" + str(time.time() - beginTime) + "\n------------------------------------------\n")


# Test dataframe accuracy
print(df_read)
