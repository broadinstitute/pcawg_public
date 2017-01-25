#!/bin/bash

rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4 /opt2

find /opt2/pcawg_pipeline/docker/modules -ignore_readdir_race  -name '*_mcr'  -exec rm -rf {} \;

#last line