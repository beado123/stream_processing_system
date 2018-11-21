#!/usr/bin/env bash
USER=$1
VM1=$2
VM2=$3
VM3=$4
VM4=$5

VM1=$USER"@fa18-cs425-g27-"$VM1".cs.illinois.edu"
VM2=$USER"@fa18-cs425-g27-"$VM2".cs.illinois.edu"
VM3=$USER"@fa18-cs425-g27-"$VM3".cs.illinois.edu"
VM4=$USER"@fa18-cs425-g27-"$VM4".cs.illinois.edu"

rsync -avP --no-links --exclude-from='../tools/rsync_exclude.txt' ~/ece428/mp3 $VM1:
if [ -z "$VM2" ]; then rsync -avP --no-links --exclude-from='../tools/rsync_exclude.txt' ~/ece428/mp3 $VM2:; fi
if [ -z "$VM3" ]; then rsync -avP --no-links --exclude-from='../tools/rsync_exclude.txt' ~/ece428/mp3 $VM3:; fi
if [ -z "$VM4" ]; then rsync -avP --no-links --exclude-from='../tools/rsync_exclude.txt' ~/ece428/mp3 $VM4:; fi
