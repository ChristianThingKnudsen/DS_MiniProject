# DS_MiniProject

Distributed storage mini project.... Yay....

## Connecting to raspbery pi's

- pi1: ssh pi@192.168.0.100
- pi2: ssh pi@192.168.0.101
- pi3: ssh pi@192.168.0.102
- pi4: ssh pi@192.168.0.103
- password: raspberry

## Running servers
- rest-server: python rest-server.py
- storage-node: python storage-node.py

## Copy files to raspberry pi's using script 
- bash transfer_files.sh


## Create new db
sqlite3.exe files.db ".read create_table.sql"

## Generating SSH-key
- ssh-keygen -t ed25519 -C "your_email@example.com" => giver .pub (public fil + path) 
- eval "$(ssh-agent -s)" => starter ssh agent på pi'en
- cat + .pub path => kopier dette 
- github => settings + add ssh-key 

## KODO 
- install 1: python waf configure build
- install 2: python waf install
- copy .so file: cp ./kodo.cpython-37m-arm-linux-gnueabihf.so ./DS