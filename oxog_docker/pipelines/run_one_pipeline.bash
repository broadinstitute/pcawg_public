#!/bin/bash
export NAMESPACE=$1 #unused?
export PIPELINE=$2
export INDIVIDUAL_ID=$3
export BAM_TUMOR=$4
export BAM_NORMAL=$5


export PIPETTE_SERVER_DIR=/cga/fh/pcawg_pipeline/utils/pipette_server
export TIMESTAMP=`date +'%Y-%m-%d__%H-%M-%S'`
#COMMDIR contains ongoing status in files under $COMMDIR/report
export COMMDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/status
#OUTDIR contains the intermediate files
export OUTDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/$INDIVIDUAL_ID
#FINALRESULTSDIR contains all the files that should be kept after the pipeline completes
export FINALRESULTSDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/results

rm -rf $COMMDIR
mkdir -p $COMMDIR

python3 $PIPETTE_SERVER_DIR/pipetteSynchronousRunner.py $COMMDIR $OUTDIR $PIPELINE $COMMDIR $OUTDIR $INDIVIDUAL_ID $BAM_TUMOR $BAM_NORMAL "${@:6}"


# note: you can see the status of running pipelines by running one of the following in a separate shell on the docker image
# $COMMDIR/report/pipeline_summary.status.txt and $COMMDIR/report/job.status.txt are updated every 10 seconds while the pipeline runs
#
# while true; do clear ; date; cat /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/pipeline_summary.status.txt; sleep 10 ; done
#
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|grep RUN; sleep 10 ; done

#Failing modules, (though clears once pipeline halts)
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|egrep '(Fail|Abort)' ; sleep 10 ; done

#Package up the final results
mkdir -p $FINALRESULTSDIR

#collect job status outuputs into one file
#keep just one header, at the top; sort all fails to the top, sort by module subdirs within passing and failing.
find $OUTDIR -name pipette.module.usage.txt  | xargs  sh -c 'for f; do cat "$f" ; done' true |sort | uniq > $FINALRESULTSDIR/$INDIVIDUAL_ID.summary.usage.txt

#collect the file outputs for distribution to the PCAWG network
tar -cvhf $FINALRESULTSDIR/$INDIVIDUAL_ID.gnos_files.tar $OUTDIR/links_for_gnos

#display any failing modules
grep FAIL $FINALRESULTSDIR/$INDIVIDUAL_ID.summary.usage.txt

#last line