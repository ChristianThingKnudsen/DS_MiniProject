#!/bin/sh
echo "PI:192.168.0.100"
pscp -pw "raspberry" pi@192.168.0.100:DS/*.csv ./measurements/EC_a/temp/100/
echo "PI:192.168.0.101"
pscp -pw "raspberry" pi@192.168.0.101:DS/*.csv ./measurements/EC_a/temp/101/
echo "PI:192.168.0.102"
pscp -pw "raspberry" pi@192.168.0.102:DS/*.csv ./measurements/EC_a/temp/102/
echo "PI:192.168.0.103"
pscp -pw "raspberry" pi@192.168.0.103:DS/*.csv ./measurements/EC_a/temp/103/
echo "Files retrieved from all PI's"