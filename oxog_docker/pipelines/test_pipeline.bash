#!/bin/bash
export NAMESPACE=$1 #unused?
export PIPELINE=$2
export PIPETTE_SERVER_DIR=/cga/fh/pcawg_pipeline/utils/pipette_server
export TIMESTAMP=`date +'%Y-%m-%d__%H-%M-%S'`
#COMMDIR contains ongoing status in files under $COMMDIR/report
export COMMDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/status
#OUTDIR contains the intermediate files
#FINALRESULTSDIR contains all the files that should be kept after the pipeline completes
export FINALRESULTSDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/results
rm -rf $COMMDIR
mkdir -p $COMMDIR

python3 $PIPETTE_SERVER_DIR/pipetteServer.py -c /cga/fh/pcawg_pipeline/jobResults_pipette/status &

run_one (){

OUTDIR=/cga/fh/pcawg_pipeline/jobResults_pipette/jobs/$1
python3 /cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py $COMMDIR $OUTDIR "$@"
sleep 10

}

sleep 10

#run_one 945e6808-2887-4f32-ac32-8f9e00353bf6 /cgaext/tcga/can2_store/e144c843-5043-4fb7-ab39-128ca91ffe/e144c843-5043-4fb7-ab39-128ca91ffe92/PCAWG.abd8a814-8c7d-4b4a-b4a7-38394a36ac30.bam /cgaext/tcga/can2_store/bd829214-f230-4331-b234-def10bbe79/bd829214-f230-4331-b234-def10bbe7938/PCAWG.ba8bb154-ef77-4383-99b5-0abc5034aaeb.bam 27.29 /tmp/test_data/9_samples_all_centers/945e6808-2887-4f32-ac32-8f9e00353bf6*
#run_one 9ff21093-58d7-4b69-aade-c242a383ea56 /cgaext/tcga/can2_store/bf95e410-b371-406c-a192-391d2fce94/bf95e410-b371-406c-a192-391d2fce94b2/PCAWG.dc9dd886-5c1b-4564-ba84-fa2a70cc4ffe.bam /cgaext/tcga/can2_store/0074e250-33ea-4530-b716-aede78a6a4/0074e250-33ea-4530-b716-aede78a6a443/PCAWG.94f8d946-a92e-4d2b-9f29-8e10f4274efe.bam 29.81 /tmp/test_data/9_samples_all_centers/9ff21093-58d7-4b69-aade-c242a383ea56* dkfz broad sanger
##run_one 786fc3e4-e2bf-4914-9251-41c800ebb2fa /cgaext/tcga/can2_store/000e9e28-7d6d-44f5-b637-ddbd62699d/000e9e28-7d6d-44f5-b637-ddbd62699db7/PCAWG.02bf0d26-f948-4658-b871-748f6c488948.bam /cgaext/tcga/can2_store/9129813f-c196-49bb-b645-2257b5e134/9129813f-c196-49bb-b645-2257b5e134b6/PCAWG.3185c99a-b8ff-46a6-bd0a-c78bf9ddb24a.bam 32.31 /tmp/test_data/9_samples_all_centers/786fc3e4-e2bf-4914-9251-41c800ebb2fa*
#run_one 8d4cb709-c95c-4bdc-844b-c0bfa2a3028e /cgaext/tcga/can2_store/906812ff-28fb-4ecd-8040-90b09278d7/906812ff-28fb-4ecd-8040-90b09278d7df/PCAWG.c95d9015-10a2-45bb-bc00-93e1c5815a43.bam /cgaext/tcga/can2_store/a80881bf-3bf3-4597-974b-7621d1ccb1/a80881bf-3bf3-4597-974b-7621d1ccb18e/PCAWG.4a47eec1-a25e-4591-be8e-110637134b0e.bam 35.82 /tmp/test_data/9_samples_all_centers/8d4cb709-c95c-4bdc-844b-c0bfa2a3028e*
##run_one 6aa00162-6294-4ce7-b6b7-0c3452e24cd6 /cgaext/tcga/can2_store/b13d6556-5efa-4580-924d-30fc27c86a/b13d6556-5efa-4580-924d-30fc27c86aef/PCAWG.6e51117c-0705-4967-b674-e4aed6038f8b.bam /cgaext/tcga/can2_store/616b79ad-38e6-4715-90f8-ce010e19bb/616b79ad-38e6-4715-90f8-ce010e19bb58/PCAWG.7aa1e116-1111-4acb-b368-578d10458cd0.bam 38.29 /tmp/test_data/9_samples_all_centers/6aa00162-6294-4ce7-b6b7-0c3452e24cd6*
#run_one 252a1c43-f954-44d7-8e31-6bcd0157a05c /cgaext/tcga/can2_store/11d59712-2aa8-40e8-8e93-3db41dcde7/11d59712-2aa8-40e8-8e93-3db41dcde710/PCAWG.964eb3a2-8c65-46b1-9087-14b3b5ade6ad.bam /cgaext/tcga/can2_store/f6a408bf-f5ee-4618-a00e-c616400531/f6a408bf-f5ee-4618-a00e-c61640053196/PCAWG.80d8e24d-3ec1-4e43-a28a-4dba015c7fa6.bam 38.59 /tmp/test_data/9_samples_all_centers/252a1c43-f954-44d7-8e31-6bcd0157a05c*
#run_one 8853cbee-7931-49a6-b063-a806943a10ad /cgaext/tcga/can2_store/ee770885-b07c-4237-ae57-6eb5211144/ee770885-b07c-4237-ae57-6eb52111446d/PCAWG.c44b8511-615b-45c2-b848-ac4a419e307e.bam /cgaext/tcga/can2_store/7634a2d6-33d6-4d5a-9780-e62935985c/7634a2d6-33d6-4d5a-9780-e62935985cc1/PCAWG.71481425-8371-4e2f-be7f-22ef1883af87.bam 38.59 /tmp/test_data/9_samples_all_centers/8853cbee-7931-49a6-b063-a806943a10ad*
#run_one 9d29543e-8601-4fd0-8e76-3df3de465cab /cgaext/tcga/can2_store/ab98704c-5a3d-494d-ba3b-85a5c37b08/ab98704c-5a3d-494d-ba3b-85a5c37b0828/PCAWG.df7ec290-c137-4a78-93f2-c0e15bf6b3b1.bam /cgaext/tcga/can2_store/4f58250a-9ebe-4a5b-9c6a-f804be2d1a/4f58250a-9ebe-4a5b-9c6a-f804be2d1add/PCAWG.88eb7362-ea2f-43e7-861d-e9b70dce04f3.bam 39.89 /tmp/test_data/9_samples_all_centers/9d29543e-8601-4fd0-8e76-3df3de465cab*
#run_one b3b3a27c-ee9a-42af-a6d1-9af5970a98b9 /cgaext/tcga/can2_store/4eda8fde-9820-4062-9706-45886bdf54/4eda8fde-9820-4062-9706-45886bdf548c/PCAWG.0b2484f8-2bc8-474b-830a-42c68c78c881.bam /cgaext/tcga/can2_store/1e8faf9d-5754-4c99-a099-7d16c68a64/1e8faf9d-5754-4c99-a099-7d16c68a64ad/PCAWG.3b2c5881-e2a9-4ae9-9abd-bafec7c045f1.bam 39.95 /tmp/test_data/9_samples_all_centers/b3b3a27c-ee9a-42af-a6d1-9af5970a98b9*
#run_one d8f0becd-fda8-41f4-a424-e082f9eae22c /cgaext/tcga/can2_store/10209e5b-63cd-49c8-b537-037e946a80/10209e5b-63cd-49c8-b537-037e946a806c/PCAWG.53d73a93-31ca-480e-ab1b-36742dcde99d.bam /cgaext/tcga/can2_store/87eb7494-f142-49a7-8a5e-4f862311d4/87eb7494-f142-49a7-8a5e-4f862311d40c/PCAWG.2ebf592b-a06d-442c-8b79-743df0d2d0f0.bam 39.68 /tmp/test_data/broad_samples/
# note: you can see the status of running pipelines by running one of the following in a separate shell on the docker image
# $COMMDIR/report/pipeline_summary.status.txt and $COMMDIR/report/job.status.txt are updated every 10 seconds while the pipeline runs
#
# while true; do clear ; date; cat /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/pipeline_summary.status.txt; sleep 10 ; done
#
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|grep RUN; sleep 10 ; done

#Failing modules, (though clears once pipeline halts)
# while true; do clear ; cat  /cga/fh/pcawg_pipeline/jobResults_pipette/status/report/job.status.txt|egrep '(Fail|Abort)' ; sleep 10 ; done