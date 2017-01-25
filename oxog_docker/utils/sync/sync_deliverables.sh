#!/bin/bash



rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/docker/ /opt2/pcawg_pipeline/docker

find /opt2/pcawg_pipeline/docker/modules -ignore_readdir_race  -name '*_mcr'  -exec rm -rf {} \;


rsync -avh --exclude ".*"  --exclude "junk"  gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/refdata/ /opt2/pcawg_pipeline/refdata

rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/test_data/ /opt2/pcawg_pipeline/test_data


#last line