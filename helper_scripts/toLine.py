from glob import glob
import time

timerStart=time.time()
data=open("ref_file_name.fa").readlines()
for n,line in enumerate(data):
    if line.startswith("line"):
       data[n] = "\n"+line.lower().rstrip()
    else:
       data[n]=line.lower().rstrip()

with open('ref_file.fa', 'w') as out:
     out.writelines(''.join(data))

timerEnd=time.time()
print timerEnd-timerStart
