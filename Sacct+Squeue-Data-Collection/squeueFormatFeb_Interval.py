#iterates through folder and based off of the name, then creates a new file and adds corresponding names with correct formatting
import os
import datetime as dt
import re

path = "/oasis/scratch/comet/mkandes/temp_project/data/comet/slurm/squeue/feb-2019"

startDay = 20190201  #choose 
endDay = 20190201
diffDay = endDay - startDay + 1

delimKey = []   
first = 1
febuary = []
weeks = []
interval = 1 #this is the interval for dividing the month in units days
start = "febDay"  #these are for naming purposes
ending = ".dat"
headers = list(open(path + "/" + (os.listdir(path))[0], "r").readline())

for index, char in enumerate(headers):
    if (char != ' ' and headers[index - 1] == ' ') and (char != "\n"):
        headers.insert(index, "|")
        delimKey.append(index)

print(delimKey)
""
n = 0
day = startDay

while n < diffDay:   #populates list with days in febuary for later check
    if ((n % interval) == 0 ): #because list of list is empty and will allow to work for mult. monthes
        febuary.append(list())
        newFile = open("-" + str(day) + "-I-" + str(interval) , "a")
        newFile.close()
    febuary[int(n / interval)].append(str(day))
    n = n + 1
    day = day + 1
print(febuary)
lineList = list()   

def timeToHours( i ):
    if i == "0:00" :
        return 0.0
    elif len(i) == 10:
        w = ((float(i[0])*24) + float(i[2:3]) + (float(i[5:6])/60) + (float(i[8:9])/360))
        return w
    elif len(i) == 8:
        w = (float(i[0:2]) + (float(i[3:5])/60) + (float(i[6:8])/360))
        return w
    elif len(i) == 7:
        w = (float(i[0]) + (float(i[2:4])/60) + (float(i[5:7])/360))
        return w
    elif len(i) == 5:
        w = ((float(i[0:2])/60) + (float(i[3:5])/360))
        return w
    elif (str(i) == "None") or (str(i) == ""):
        w = 0.0
        noneCount = noneCount + 1
        return w
    elif len(i) == 4:
        w = ((float(i[0])/60) + (float(i[2:4])/360))
        return w

for roots, dirs, files in os.walk(path, topdown = True): 
    for file in files: #iterates through all files in directory
        uTime = int(file[7:17])
        name ="%s" % file  #files name as string
        for he in febuary:
            for q in he:
                if ((q + "T") in name) :
                    newFile = open( "-"+ febuary[febuary.index(he)][0] + "-I-" + str(interval) , "a") #creates new file
                    mainFile = open(path + "/" + file, "r") #opens file to read
                    next(mainFile)
                    for line in mainFile:
                        nline = list(line)
                        for i in delimKey:
                            nline.insert(i, '|')
                        column = 0
                        time = ""
                        timeInterval = ""
                        nline = (''.join(nline))
                        thisLine = ''.join(nline.split())
                        
                        for x in thisLine:
                            if column == 11:
                                time = time + x
                            elif column == 12:
                                timeInterval = timeInterval + x
                            if x == "|":
                                column = column + 1
                        timeInterval = timeInterval.replace("|", "")
                        time = time.replace("|", "")
                        print("noChange: " + thisLine)
                        thisLine = thisLine.replace("|" + time + "|", "|" + str(timeToHours(time)) + "|", 1)
                        print("timeFixed:"+ thisLine)
                        thisLine = thisLine.replace("|" + timeInterval + "|","|" + str(timeToHours(timeInterval)) + "|", 1)
                        print("Both?" + thisLine)
                        thisLine = str(uTime) + '|' + thisLine  # Inserting Unix time for each line.
                        newFile.write(thisLine + '\n')
                    first = first + 1  #first is to determine what file number we are on
                    mainFile.close() 
                    newFile.close()
                else:
                    pass
print(first) #prints the number of files, I believe the number is 8036

for it in febuary:  #renames file 
    thisFile = open("-"+ febuary[febuary.index(it)][0] + "-I-" + str(interval), "r")
    firstLine = thisFile.readline()
    unixT = firstLine.split("|", 1)[0]
    thisFile.close()
    thisName = "-"+ febuary[febuary.index(it)][0] + "-I-" + str(interval)
    
    oldPath = os.path.abspath(thisName)
    print(oldPath)
    newPath = re.sub(thisName, str(unixT) + thisName, oldPath)
    os.rename(os.path.abspath(thisName), newPath)

               

