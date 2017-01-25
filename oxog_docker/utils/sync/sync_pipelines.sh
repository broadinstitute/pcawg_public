#!/bin/bash

rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/docker/pipelines/ /opt2/pcawg_pipeline/docker/pipelines
rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/docker/utils/ /opt2/pcawg_pipeline/docker/utils
rsync -avh --exclude ".*" gsaksena@69.173.70.43:/cga/fh/pcawg_pipeline4/docker/Dockerfile /opt2/pcawg_pipeline/docker/Dockerfile

#last line