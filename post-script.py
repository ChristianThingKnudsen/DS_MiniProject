# Script for making post requests to lead node. 
import requests
import time
import sys
# Files 
file_names = ["10KB.bin", "100KB.bin", "1MB.bin", "10MB.bin", "100MB.bin"]
file_name = file_names[0]
# Replicas
replicas = [2,3,4]
n_replica = replicas[2]
# Storage types
index = 0
storage_types = ["HDFS", "RAID1", "EC_RS"]
storage_type = storage_types[index]
# Bodies
HDFS_body = {
    "n_replicas": n_replica, 
    "storage": storage_type, # Should be HDFS
}
RAID_body = {
    "n_replicas": n_replica, 
    "storage": storage_type, # Should be RAID1
}
EC_body = {
    "storage": storage_type, 
    "max_erasures": 2, # Loss tolerance.
    "type": "b" # can be 'a' or 'b'. 
}
bodies = [HDFS_body,RAID_body, EC_body]
body = bodies[index]

url = "http://192.168.0.100:5000/files"
file = open("../"+file_name,"rb")
files =  {
        "file": file
}
for i in range(100): 
    file.seek(0,0)
    if storage_type == "HDFS":
        t1 = time.time()
        response = requests.post(url, files=files, data = body)
        f = open("./measurements/HDFS/HDFS_results_"+str(n_replica)+"k_"+ file_name.split(".")[0] + ".csv", "a")
        f.write(str(time.time()-t1) + "\n")
        f.close()
        sys.stdout.write("completed request for " +file_name+ " "+ "time: "+ str(time.time()-t1)+ " iteration: "+ str(i) + "\n")  # same as print
        sys.stdout.flush()
        time.sleep(1)
    elif storage_type == "RAID1":
        t1 = time.time()
        response = requests.post(url, files=files, data = body)
        f = open("./measurements/RAID1/RAID1_results_"+str(n_replica)+"k_"+ file_name.split(".")[0] + ".csv", "a")
        f.write(str(time.time()-t1) + "\n")
        f.close()
        sys.stdout.write("completed request for " +file_name+ " "+ "time: "+ str(time.time()-t1)+ " Type: " + storage_type + " iteration: "+ str(i) + "\n")  # same as print
        sys.stdout.flush()
    elif storage_type == "EC_RS":
        t1 = time.time()
        response = requests.post(url, files=files, data = body)
        f = open("./measurements/EC_"+body["type"]+"/EC_"+body["type"]+"_results_"+str(body["max_erasures"])+"l_"+ file_name.split(".")[0] + ".csv", "a")
        f.write(str(time.time()-t1) + "\n")
        f.close()
        sys.stdout.write("completed request for " +file_name+ " "+ "time: "+ str(time.time()-t1)+ " Type: " + storage_type + " iteration: "+ str(i) + "\n")  # same as print
        sys.stdout.flush()
        time.sleep(1)
    else:
        print("Unnown storage type")
        break        
print("Done John")