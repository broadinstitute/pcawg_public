workflow pcawg_oxog_workflow {
        call pcawg_oxog
}

task pcawg_oxog {

        #Define workflow parameters within the task
        String pairID
        File bam_tumor
        File bam_tumor_index
        File refdata1
        String oxoq
        File input_vcf_gz
        File input_vcf_gz_tbi


        String output_disk_gb
        String boot_disk_gb = "10"
        String ram_gb
        String cpu_cores


    command {
python_cmd="
import subprocess
def run(cmd):
    print(cmd)
    subprocess.check_call(cmd,shell=True)

run('ln -sTf `pwd` /opt/execution')
run('ln -sTf `pwd`/../inputs /opt/inputs')
# run('/cga/fh/pcawg_pipeline/utils/monitor_start.py')

# start task-specific calls
##########################

#copy wdl args to python vars
pairID = '${pairID}'
bam_tumor = '${bam_tumor}'
bam_tumor_index = '${bam_tumor_index}'
oxoq = '${oxoq}'
input_vcf_gz = '${input_vcf_gz}'
input_vcf_gz_tbi = '${input_vcf_gz_tbi}'

refdata1='${refdata1}'



import os
import sys
import tarfile
import shutil



#define the pipeline
PIPELINE='/cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py'

#define the directory for the pipette server to allow the pipette pipelines to run
PIPETTE_SERVER_DIR='/cga/fh/pcawg_pipeline/utils/pipette_server'

#define the location of the directory for communication data
cwd = os.getcwd()
COMMDIR=os.path.join(cwd,'pipette_status')
OUTDIR=os.path.join(cwd,'pipette_jobs')
REFDIR = os.path.join(cwd,'refdata')
INPUTS=os.path.join(cwd,'inputs')
OUTFILES = os.path.join(cwd,'output_files')

if os.path.exists(COMMDIR):
    shutil.rmtree(COMMDIR)
os.mkdir(COMMDIR)

if not os.path.exists(INPUTS):
    os.mkdir(INPUTS)
if not os.path.exists(OUTFILES):
    os.mkdir(OUTFILES)

if not os.path.exists(REFDIR):
    os.mkdir(REFDIR)
    # unpack reference files
    run('tar xvf %s -C %s'%(refdata1,REFDIR))

#colocate the indexes with the bams via symlinks
TUMOR_BAM = os.path.join(INPUTS,'tumor.bam')
TUMOR_INDEX = os.path.join(INPUTS,'tumor.bam.bai')

if not os.path.exists(TUMOR_BAM):
    os.link(bam_tumor,TUMOR_BAM)
    os.link(bam_tumor_index,TUMOR_INDEX)

INPUT_VCF_GZ = os.path.join(INPUTS,'input.vcf.gz')
INPUT_VCF_GZ_TBI = os.path.join(INPUTS,'input.vcf.gz.tbi')
if not os.path.exists(INPUT_VCF_GZ):
    os.link(input_vcf_gz,INPUT_VCF_GZ)
    os.link(input_vcf_gz_tbi,INPUT_VCF_GZ_TBI)



#run the pipette synchronous runner to process the test data
cmd_str = 'python3 %s/pipetteSynchronousRunner.py '%PIPETTE_SERVER_DIR + ' '.join([COMMDIR,OUTDIR,PIPELINE,COMMDIR,OUTDIR,pairID,TUMOR_BAM,oxoq,INPUT_VCF_GZ,'--ref',REFDIR])





pipeline_return_code = subprocess.call(cmd_str,shell=True)

# capture module usage
mufn = 'pipette.module.usage.txt'
mus = []
for root, dirs, files in os.walk(OUTDIR):
    if mufn in files:
        fid = open(os.path.join(root,mufn))
        usageheader = fid.readline()
        usage = fid.readline()
        mus.append(usage)
mus.sort()
# output usage for failures to stdout
for line in mus:
    if 'FAIL' in line:
        sys.stderr.write (line)
# tar up failing modules
with tarfile.open('failing_intermediates.tar','w') as tar:
    for line in mus:
        line_list = line.split()
        if line_list[0] == 'FAIL':
            module_outdir = line_list[2]
            tar.add(module_outdir)


# write full file to output
fid = open(os.path.join(OUTFILES,'%s.summary.usage.txt'%pairID),'w')
fid.write(usageheader)
fid.writelines(mus)
fid.close()


#########################
# end task-specific calls
# run('/cga/fh/pcawg_pipeline/utils/monitor_stop.py')

"
        echo "$python_cmd"
        python -c "$python_cmd"


    }

        parameter_meta{

                pairID: "The ID of the pair of bam files that are analyzed"
                bam_tumor: "The tumor genome sample analyzed in the pipeline"
                bam_normal: "The normal genome sample analyzed in the pipeline"
                bam_tumor_index: "The bam file index for the tumor sample bam file"
                bam_normal_index: "The bam file index for the normal sample bam file"
                refdata1: "tar.gz file of reference data"
                diskSize: "The size of the disk allocated to the root directory, which can be changed to accomodate the size of the bam files used"
        }

        output {

        #usage
        File summary_usage="output_files/sample.summary.usage.txt"
        File dstat_log="dstat.log"
        File dstat_full_log="dstat_full.log"
        File monitor_start_log="monitor_start.log"
        File monitor_stop_log="monitor_stop.log"

        File failing_intermediates="failing_intermediates.tar"



        }

        runtime {

        docker : "docker.io/broadinstitute/pcawg_public:latest"
        memory: "${ram_gb}GB"
        cpu: "${cpu_cores}"
        disks: "local-disk ${output_disk_gb} HDD"
        bootDiskSizeGb: "${boot_disk_gb}"
        preemptible: 1
        }

        meta {
                author : "Gordon Saksena"
                email : "gsaksena@broadinstitute.org"
        }

}
