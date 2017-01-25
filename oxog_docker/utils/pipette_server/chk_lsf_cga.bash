#!/bin/bash
##
## This is a hack attempting to circumvent missing /broad/lsf
## filesystems.				matter 20090728
##
## Updated to include Cancer mount points. gsaksena 20091106, stransky 2010.02
##
if  [ -d /broad/lsf/ ]  && [ -d /xchip/cga_home ] && [ -d /home/unix ] && [ -d /xchip/tcga ] &&  [ -d /broad/hptmp ] && [ -d /seq/picard_aggregation ] && [ -d /xchip/cancergenome ] 
then
  exit 0
else
  exit 1
fi
