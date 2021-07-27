import pandas as pd

file_path = input('Path of parquet to be read: ')
display_rows = ''

while type(display_rows) != int:
    try:
        display_rows = int(input('Number of rows to display: '))
    except:
        print('Input not an integer.')


df = pd.read_parquet(file_path)

print(df.head(display_rows))

