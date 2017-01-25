#!/bin/csh
setenv LIBDIR $1
setenv FH_OUTDIR $2
setenv LABEL $3
setenv PIPELINE "$4"
setenv ARGS "$5"
setenv TIMESTAMP `date +'%Y-%m-%d__%H-%M-%S'`
setenv COMMDIR /xchip/cga_home/gsaksena/pipette/firehose/$LABEL/commdir_$TIMESTAMP

umask 002
mkdir -p $COMMDIR

use Python-3.1
python3 ${LIBDIR}pipetteSynchronousRunner.py $COMMDIR $FH_OUTDIR $PIPELINE $COMMDIR $FH_OUTDIR $ARGS

#last line