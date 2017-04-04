workflow pcawg_oxog_workflow {
        call pcawg_oxog
}

task pcawg_oxog {

        #Define workflow parameters within the task
        String pairID = "sample"
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
run('/cga/fh/pcawg_pipeline/utils/monitor_start.py')

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



def make_links(subpaths, new_names=None):
    for i,subpath in enumerate(subpaths):
        if not os.path.exists(subpath):
            sys.stderr.write ('file not found: %s'%subpath)
            continue
        if new_names:
            fn = new_names[i]
        else:
            fn = os.path.basename(subpath)
        realsubpath = os.path.realpath(subpath)
        new_path = os.path.join(OUTFILES,fn)
        if os.path.exists(new_path):
            sys.stderr.write('file already exists: %s'%new_path)
            continue
        os.link(realsubpath,new_path) #hard link, to survive export


subpaths = [
    'pipette_jobs/links_for_gnos/oxoG/sample.oxoG.tar.gz',
    'pipette_jobs/links_for_gnos/annotate_failed_sites_to_vcfs/input.oxoG.vcf.gz',
    'pipette_jobs/links_for_gnos/annotate_failed_sites_to_vcfs/input.oxoG.vcf.gz.tbi',
    'pipette_jobs/oxoG/sample.oxoG3.maf.annotated.all.maf.annotated'
]
new_names = [
    'sample.oxoG.supplementary.tar.gz',
    'sample.oxoG.vcf.gz',
    'sample.oxoG.vcf.gz.tbi',
    'sample.oxoG.maf'
]
make_links(subpaths,new_names)





#########################
# end task-specific calls
run('/cga/fh/pcawg_pipeline/utils/monitor_stop.py')

"
        echo "$python_cmd"
        python -c "$python_cmd"


    }

        parameter_meta{
                bam_tumor: "The tumor genome sample analyzed in the pipeline"
                bam_tumor_index: "The bam file index for the tumor sample bam file"
                refdata1: "tar.gz file of reference data"
                oxoq: "Value of the OxoQ metric for bam_tumor. (<20 results in heavy filtering, 40+ results in minimal filtering)"
                output_disk_gb: "The size of the disk allocated to the root directory, which can be changed to accomodate the size of the bam files used"
                input_vcf_gz: "Tabix-compressed VCF of variants to filter"
                input_vcf_gz_tbi: "Index for tabix-compressed VCF"
                ram_gb: "GB RAM for VM"
                cpu_cores: "Number of cores for VM"
        }

        output {

        #usage
        File summary_usage="output_files/sample.summary.usage.txt"
        File dstat_log="dstat.log"
        File dstat_full_log="dstat_full.log"
        File monitor_start_log="monitor_start.log"
        File monitor_stop_log="monitor_stop.log"

        File failing_intermediates="failing_intermediates.tar"

        File oxoG_supplementary_tar_gz = "output_files/sample.oxoG.supplementary.tar.gz"
        File oxoG_vcf_gz = "output_files/sample.oxoG.vcf.gz"
        File oxoG_vcf_gz_tbi = "output_files/sample.oxoG.vcf.gz.tbi"
        File oxoG_maf = "output_files/sample.oxoG.maf"

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
