import os
import datetime as dt

# Function converts YYYYMMDD time to UNIX timestamp
def tStamp(date):
    return int((date - dt.datetime(1970, 1, 1)).total_seconds())

""" Input dates here for inclusive date search range (YYYY, MM, DD, Hrs, Mins, Secs, Mill), 
can use datetimes as accurate as in the milliseconds. Remember that inputs are read as UTC-based Epochs"""
sDate = dt.datetime(2019, 2, 1, 0)  # Start of range
eDate = dt.datetime(2019, 3, 1, 0)  # End of range

# Convert both dates to unix time stamps.
sDate = tStamp(sDate)
eDate = tStamp(eDate)

inputPath = '/oasis/scratch/comet/mkandes/temp_project/data/comet/slurm/squeue/feb-2019/'  # Location of Folder

outp = open("Feb-2019_outPut.txt", 'w')  # Output txt file

# First file in folder opened to first line containing headers.
headers = list(open(inputPath + (os.listdir(inputPath))[0], "r").readline())

# List created to store indexes for future delimiters
delimKey = []

"""For loop iterates through header line once to fill out delimKey, which will be a reference for all future files and 
lines. This method solves problems of missing data values or ones that aren't separated by a space due to overly long 
values."""
for index, char in enumerate(headers):
    if char != ' ' and headers[index - 1] == ' ':
        headers.insert(index, "|")
        delimKey.append(index)

"""For loop iterates through each line in each file, inserting a '|' at indexes determined by the list configured in 
the last for loop. """
for file in os.listdir(inputPath):

    uTime = int(file[7:17])  # Unix time stamp saved from file name.

    if sDate < uTime < eDate:  # Check if file is in date range.

        inp = open(inputPath + file, 'r')
        for line in inp:
            nline = list(line)
            for i in delimKey:
                nline.insert(i, '|')\

            """This converts the list version of the line into a string, removes all spaces and writes the 
            result to the output file"""
            nline = (''.join(nline))

            nline = str(uTime) + '|' + nline  # Inserting Unix time for each line.

            outp.write(''.join(nline.split()) + '\n')

outp.close()
