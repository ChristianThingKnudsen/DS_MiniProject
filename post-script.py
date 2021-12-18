import requests
import time

file_names = ["10KB.bin", "100KB.bin", "1MB.bin", "10MB.bin", "100MB.bin"]
file_name = file_names[0]

replicas = [2,3,4]
n_replica = replicas[0]

url = "http://192.168.0.100:5000/files"
body = {
    "n_replicas": n_replica,
    "storage": "HDFS"
}
files =  {
    "file": open("../"+file_name,"rb")
}
for i in range(100):
    time.sleep(1)
    t1 = time.time()
    response = requests.post(url, files=files, data = body,headers={'Cache-Control': 'no-cache'})
    f = open("./measurements/HDFS/HDFS_results_"+str(n_replica)+"k_"+ file_name.split(".")[0] + ".csv", "a")
    f.write(file_name + ';' + str(time.time()-t1) + "\n")
    f.close()
    print("completed request for " +file_name+ ": "+ str(i))
            
print("Done John")