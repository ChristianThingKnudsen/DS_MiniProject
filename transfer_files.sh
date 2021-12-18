#!/bin/sh
echo "PI:192.168.0.100"
pscp -pw "raspberry" -r ./* pi@192.168.0.100:DS 
echo "PI:192.168.0.101"
pscp -pw "raspberry" -r ./* pi@192.168.0.101:DS
echo "PI:192.168.0.102"
pscp -pw "raspberry" -r ./* pi@192.168.0.102:DS 
echo "PI:192.168.0.103"
pscp -pw "raspberry" -r ./* pi@192.168.0.103:DS 
echo "Files transfered to all PI's"