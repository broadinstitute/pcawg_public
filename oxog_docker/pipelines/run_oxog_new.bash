#!/bin/bash
export PIPELINE="/cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py"
export INDIVIDUAL_ID=$1
export BAM_TUMOR=$2
export REFDATA=$3

export WORKDIR=$4
export COMMDIR=$WORKDIR/status
export OUTDIR=$WORKDIR/pipette/jobs


export PIPETTE_SERVER_DIR=/cga/fh/pcawg_pipeline/utils/pipette_server

#COMMDIR contains ongoing status in files under $COMMDIR/report
#export COMMDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/status
#OUTDIR contains the intermediate files
#export OUTDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/$INDIVIDUAL_ID
#FINALRESULTSDIR contains all the files that should be kept after the pipeline completes
export FINALRESULTSDIR=$5

rm -rf $COMMDIR
mkdir -p $COMMDIR

pushd $WORKDIR
tar xvf $REFDATA
popd

python3 $PIPETTE_SERVER_DIR/pipetteSynchronousRunner.py $COMMDIR $OUTDIR $PIPELINE $COMMDIR $OUTDIR $INDIVIDUAL_ID $BAM_TUMOR --ref $WORKDIR/refdata "${@:6}"


# note: you can see the status of running pipelines by running one of the following in a separate shell on the docker image
# $COMMDIR/report/pipeline_summary.status.txt and $COMMDIR/report/job.status.txt are updated every 10 seconds while the pipeline runs
#
# while true; do clear ; date; cat /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/pipeline_summary.status.txt; sleep 10 ; done
#
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|grep RUN; sleep 10 ; done

#Failing modules, (though clears once pipeline halts)
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|egrep '(Fail|Abort)' ; sleep 10 ; done

#Package up the final results
#mkdir -p $FINALRESULTSDIR

#collect job status outuputs into one file
#keep just one header, at the top; sort all fails to the top, sort by module subdirs within passing and failing.
#find $OUTDIR -name pipette.module.usage.txt  | xargs  sh -c 'for f; do cat "$f" ; done' true |sort | uniq > $FINALRESULTSDIR/$INDIVIDUAL_ID.summary.usage.txt

#collect the file outputs for distribution to the PCAWG network
#tar -cvhf $FINALRESULTSDIR/$INDIVIDUAL_ID.gnos_files.tar $OUTDIR/links_for_gnos

#display any failing modules
#grep FAIL $FINALRESULTSDIR/$INDIVIDUAL_ID.summary.usage.txt

cp $OUTDIR/links_for_gnos/oxoG/*.tar.gz $FINALRESULTSDIR/
cp $OUTDIR/links_for_gnos/annotate_failed_sites_to_vcfs/*.vcf.gz $FINALRESULTSDIR/
#last line
