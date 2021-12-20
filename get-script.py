import requests
import time
import sys

# Files 
file_names = ["10KB.bin", "100KB.bin", "1MB.bin", "10MB.bin", "100MB.bin"]
file_name = file_names[2]
url = f"http://192.168.0.100:5000/files/{file_name}"
# Storage types
storage_types = ["HDFS", "RAID1", "EC_RS"]
storage_type = storage_types[0]

for i in range(100): 
    t1 = time.time()
    response = requests.get(url)
    f = open("./measurements/"+storage_type+"/"+storage_type+"_access_"+ file_name.split(".")[0] + ".csv", "a")
    f.write(str(time.time()-t1) + "\n")
    f.close()
    sys.stdout.write("completed request for " +file_name+ " "+ "time: "+ str(time.time()-t1)+ " iteration: "+ str(i) + "\n")  
    sys.stdout.flush()
print("Done John")