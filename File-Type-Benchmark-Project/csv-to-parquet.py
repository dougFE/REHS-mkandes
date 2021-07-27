import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df_source = 'extended-compressTest.csv'
df_filename = 'extended-compressTest.parquet'

df = pd.read_csv(df_source)
table = pa.Table.from_pandas(df, preserve_index=True)
pq.write_table(table, df_filename)
print("Parquet written")
