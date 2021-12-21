# DS_MiniProject

## Connecting to raspbery pi's
- pi1: ssh pi@192.168.0.100
- pi2: ssh pi@192.168.0.101
- pi3: ssh pi@192.168.0.102
- pi4: ssh pi@192.168.0.103
- password: raspberry

## Running servers
- rest-server: python rest-server.py
- storage-node: python storage-node.py

## Scripts
- Transfer all files to PIs: bash transfer_files.sh
- Retrive .csv files from PIs
    - HDFS: retrieve_HDFS_files.sh
    - EC_RS type a: retrieve_ECa_files.sh
    - EC_RS type b: retrieve_ECb_files.sh

## Create new db
sqlite3.exe files.db ".read create_table.sql"

## Protobuf
- Run: protoc --python_out=. messages.proto

## Generating SSH-key
- ssh-keygen -t ed25519 -C "some_email@some_example.com" => giver .pub (public fil + path) 
- eval "$(ssh-agent -s)" => starter ssh agent på PI'en
- cat + .pub path => Copy this 
- github => settings + add ssh-key 

## KODO 
- Clone kodo using SSH-key and go into the directory
- install 1: python waf configure build
- install 2: python waf install
- copy .so file: cp ./kodo.cpython-37m-arm-linux-gnueabihf.so ./DS

## Measurements