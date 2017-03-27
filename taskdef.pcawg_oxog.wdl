workflow pcawg_oxog_workflow {
        call pcawg_oxog
}

task pcawg_oxog {

        #Define workflow parameters within the task
        String pairID
        File bam_tumor
        File bam_tumor_index
        File refdata1
        Float oxoq
        File input_vcf

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
run('/opt/src/algutil/monitor_start.py')

# start task-specific calls
##########################

#copy wdl args to python vars
pairID = '${pairID}'
bam_tumor = '${bam_tumor}'
bam_tumor_index = '${bam_tumor_index}'
oxoq = '${oxoq}'
input_vcf = '${input_vcf}'

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
INPUT_BAMS=os.path.join(cwd,'input_bams')
OUTFILES = os.path.join(cwd,'output_files')

if os.path.exists(COMMDIR):
    shutil.rmtree(COMMDIR)
os.mkdir(COMMDIR)

if not os.path.exists(INPUT_BAMS):
    os.mkdir(INPUT_BAMS)
if not os.path.exists(OUTFILES):
    os.mkdir(OUTFILES)

if not os.path.exists(REFDIR):
    os.mkdir(REFDIR)
    # unpack reference files
    run('tar xvf %s -C %s'%(refdata1,REFDIR))

#colocate the indexes with the bams via symlinks
TUMOR_BAM = os.path.join(INPUT_BAMS,'tumor.bam')
TUMOR_INDEX = os.path.join(INPUT_BAMS,'tumor.bam.bai')

if not os.path.exists(TUMOR_BAM):
    os.link(bam_tumor,TUMOR_BAM)
    os.link(bam_tumor_index,TUMOR_INDEX)




#run the pipette synchronous runner to process the test data
cmd_str = 'python3 %s/pipetteSynchronousRunner.py '%PIPETTE_SERVER_DIR + ' '.join([COMMDIR,OUTDIR,PIPELINE,COMMDIR,OUTDIR,pairID,TUMOR_BAM,oxoq,input_vcf,'--ref',REFDIR])

'''
        parser = argparse.ArgumentParser()
        parser.add_argument("--ref", default="/cga/fh/pcawg_pipeline/refdata")
        parser.add_argument("commDir")
        parser.add_argument("output")
        parser.add_argument("indiv_id")
        parser.add_argument("bam")
        parser.add_argument("oxoQ")
        parser.add_argument("vcfs", nargs="+")

        Parameters: (in order, separated by space)

aliquot_id 

mounted path to bam (must be indexed!)

path to maf

oxoQ value

mounted output directory

Putting it all together:

docker run -v PATH_WITH_DATA:PATH_WITH_DATA:ro -v PATH_WITH_REF_FASTA:/cga/fh/pcawg_pipeline/refdata/public/:ro  -v HOST_OUT_DIR:DOCKER_OUT_DIR oxogfilterfinal ALIQUOT_ID DOCKER_BAM_PATH DOCKER_MAF_PATH OXOQ_VALUE DOCKER_OUT_DIR

example:

docker run -v /cgaext/:/cgaext/:ro -v /cga/fh/pcawg_pipeline/refdata/public/:/cga/fh/pcawg_pipeline/refdata/public/:ro -v /tmp/:/opt/output oxogfilterfinal 7b296706-b535-45a0-9a8e-4a34a8c33bb5 /cgaext/tcga/pcawg/docker_results/7b/7b296706-b535-45a0-9a8e-4a34a8c33bb5/tumor.var.bam /cgaext/015-0038-01TD_015-0039-01ND.filtered.maf 38.84 /opt/output

-----
Run /cga/fh/pcawg_pipeline/pipelines/run_one_pipeline.bash 
with the following arguments:
run_one_pipeline.bash pcawg $PIPELINE $ALIQUOT $TUMOR_BAM $NORMAL_BAM $OXOQ_SCORE $VCF1 $VCF2 $VCF3

(note - BAMs must be indexed!) 

where $PIPELINE can be either one of:
/cga/fh/pcawg_pipeline/pipelines/oxog_pipeline.py
/cga/fh/pcawg_pipeline/pipelines/variantbam_pipeline.py
/cga/fh/pcawg_pipeline/pipelines/oxog_and_variantbam_pipeline.py

 $ALIQUOT is the alphanumeric aliquot to identify and name the sample by
 $TUMOR_BAM and $NORMAL_BAM are the sample bams
 $OXOQ_SCORE numeric oxoq score  #If running only variant bam, placeholder values may be used
 $VCF1 $VCF2 $VCF3 are the input snv vcfs.    #If running only variant bam, placeholder values may be used

Outputs:
/cga/fh/pcawg_pipeline/jobResults_pipette/results will contain a tarball of all the results. 
Inside you will find $ALIQUOT.oxoG.somatic.snv_mnv.vcf.gz which is the final vcf. This contains the info field “set”. 
“set=Intersection” means that the call was present in all three inputs, and passed the oxog filter.
Otherwise the value will include a list of passing vcfs, separated by a dash e.g. “sanger-oxoG_pass” means the mutation passed the oxoG filter, and was only called in the Sanger vcf. If the mutation did not pass the filter, oxoG_pass will be absent. 
---
$PIPELINE=/cga/fh/pcawg_pipeline/pipelines/variantbam_pipeline.py
$OXOQ_SCORE=PLACEHOLDER
run_one_pipeline.bash pcawg $PIPELINE $ALIQUOT $TUMOR_BAM $NORMAL_BAM $OXOQ_SCORE $VCF1 [$VCF2] [$VCF3] [$VCF4] 
---
# Generate OxoG unioned VCF and tumour mini-bam
$PIPELINE=/cga/fh/pcawg_pipeline/pipelines/oxog_and_variantbam_pipeline.py
run_one_pipeline.bash pcawg $PIPELINE $ALIQUOT $TUMOR_BAM $NORMAL_BAM $OXOQ_SCORE $VCF1 $VCF2 $VCF3

  '''



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
        new_path = os.path.join(OUTFILES,fn)
        if os.path.exists(new_path):
            sys.stderr.write('file already exists: %s'%new_path)
            continue
        os.link(subpath,new_path)

def make_archive(subpaths,archive_name):
    archive_path = os.path.join(OUTFILES,archive_name)
    with tarfile.open(archive_path,'w') as tar:
        for subpath in subpaths:
            if not os.path.exists(subpath):
                sys.stderr.write ('file not found: %s'%subpath)
            else:
                tar.add(subpath)





if 'dranger' in sub_workflow:
    subpaths = [
        'pipette_jobs/tabix_dRanger/sample.broad-dRanger.DATECODE.somatic.sv.vcf.gz',
        'pipette_jobs/tabix_dRanger/sample.broad-dRanger.DATECODE.somatic.sv.vcf.gz.tbi',
        'pipette_jobs/links_for_broad/dRanger2VCF/sample.dRanger_results.detail.txt.gz'
    ]
    make_links(subpaths)



    subpaths = ['pipette_jobs/links_for_broad/dRangerPreProcess_Normal_sg_gather/sample.all.isz.gz',
        'pipette_jobs/links_for_broad/dRangerPreProcess_Tumor_sg_gather/sample.all.isz.gz',
        'pipette_jobs/links_for_broad/BreakPointer_Normal_sg_gather/sample.breakpoints.txt.gz',
        'pipette_jobs/links_for_broad/BreakPointer_Normal_sg_gather/sample.matched.sam.gz',
        'pipette_jobs/links_for_broad/BreakPointer_Tumor_sg_gather/sample.breakpoints.txt.gz',
        'pipette_jobs/links_for_broad/BreakPointer_Tumor_sg_gather/sample.matched.sam.gz',
        'pipette_jobs/links_for_broad/dRanger2VCF/sample.dRanger_results.detail.txt.gz',
        'pipette_jobs/links_for_broad/dRanger_Finalize/sample.dRanger_results.detail.all.mat.gz',
        'pipette_jobs/links_for_broad/dRanger_Finalize/sample.dRanger_results.detail.all.txt.gz',
        'pipette_jobs/links_for_broad/dRanger_Finalize/sample.dRanger_results.detail.somatic.txt.gz',
        'pipette_jobs/links_for_broad/dRanger_Finalize/sample.dRanger_results.somatic.txt.gz',
        'pipette_jobs/links_for_broad/getdRangerSupportingReads_Tumor/sample.dRanger.supporting_reads.txt.gz',
        'pipette_jobs/links_for_broad/getdRangerSupportingReads_Normal/sample.dRanger.supporting_reads.txt.gz',
        'pipette_jobs/links_for_broad/dRangerRun/sample.dRanger_results.forBP.txt.gz',
        'pipette_jobs/links_for_broad/dRangerRun/sample.dRanger_results.mat.gz',
        'pipette_jobs/links_for_broad/dRangerRun/stderr.txt.gz',
        'pipette_jobs/links_for_broad/dRangerRun/stdout.txt.gz']
    make_archive(subpaths,'dRanger_intermediates.tar')


if 'snowman' in sub_workflow:
    subpaths = [
        #+-- tabix_snowman_germline_indel
        'pipette_jobs/tabix_snowman_germline_indel/sample.broad-snowman.DATECODE.germline.indel.vcf.gz',
        'pipette_jobs/tabix_snowman_germline_indel/sample.broad-snowman.DATECODE.germline.indel.vcf.gz.tbi',
        #+-- tabix_snowman_germline_sv
        'pipette_jobs/tabix_snowman_germline_sv/sample.broad-snowman.DATECODE.germline.sv.vcf.gz',
        'pipette_jobs/tabix_snowman_germline_sv/sample.broad-snowman.DATECODE.germline.sv.vcf.gz.tbi',
        #+-- tabix_snowman_somatic_indel
        'pipette_jobs/tabix_snowman_somatic_indel/sample.broad-snowman.DATECODE.somatic.indel.vcf.gz',
        'pipette_jobs/tabix_snowman_somatic_indel/sample.broad-snowman.DATECODE.somatic.indel.vcf.gz.tbi',
        #+-- tabix_snowman_somatic_sv
        'pipette_jobs/tabix_snowman_somatic_sv/sample.broad-snowman.DATECODE.somatic.sv.vcf.gz',
        'pipette_jobs/tabix_snowman_somatic_sv/sample.broad-snowman.DATECODE.somatic.sv.vcf.gz.tbi'
        ]
    make_links(subpaths)



    subpaths = [
        'pipette_jobs/snowman/sample.alignments.txt.gz',
        'pipette_jobs/snowman/sample.bps.txt.gz',
        'pipette_jobs/snowman/sample.cigarmap.txt.gz',
        'pipette_jobs/snowman/sample.contigs_all.sam.gz',
        'pipette_jobs/snowman/sample.contigs.bam',
        'pipette_jobs/snowman/sample.contigs.bam.bai',
        'pipette_jobs/snowman/sample.discordant.txt.gz',
        'pipette_jobs/snowman/sample.r2c.bam',
        'pipette_jobs/snowman/pipette.module.stdout.txt',
        'pipette_jobs/snowman/pipette.module.stderr.txt'
        ]
    make_archive(subpaths,'snowman_intermediates.tar')



if 'dranger_snowman_mergesvcalls' in sub_workflow:
    subpaths = [
        'pipette_jobs/tabix_merge_sv_vcf/sample.broad-dRanger_snowman.DATECODE.somatic.sv.vcf.gz',
        'pipette_jobs/tabix_merge_sv_vcf/sample.broad-dRanger_snowman.DATECODE.somatic.sv.vcf.gz.tbi'
        ]
    make_links(subpaths)




if 'forcecallhets' in sub_workflow:
    subpaths = [
        'pipette_jobs/links_for_broad/mutect_het_sites_sg_gather/sample.call_stats.txt.gz',
        'pipette_jobs/mutect_het_sites/sg/gather/sample.coverage.wig.txt.gz'
        ]
    make_archive(subpaths,'mutect_het_sites.tar')


if 'recapseg' in sub_workflow:
    subpaths = [
        'pipette_jobs/links_for_broad/re_capseg_coverage_normal_merged/sample.normal.uncorrected_target_order.coverage.gz',
        'pipette_jobs/links_for_broad/re_capseg_coverage_tumor_merged/sample.tumor.uncorrected_target_order.coverage.gz'
        ]
    make_archive(subpaths,'recapseg.tar')



if 'contest' in sub_workflow:
    subpaths = [
        'pipette_jobs/contest/tumor.bam.contamination.txt.firehose'
        ]
    make_links(subpaths)


    subpaths = [
        'pipette_jobs/contest/tumor.bam.contamination.txt.base_report.txt',
        'pipette_jobs/contest/tumor.bam.contamination.txt',
        'pipette_jobs/contest/tumor.bam.contamination.txt.out',
        'pipette_jobs/contest/stdout.txt',
        'pipette_jobs/contest/stderr.txt'
        ]
    make_archive(subpaths,'contest_intermediates.tar')




if 'mutect' in sub_workflow:
    subpaths = [
        #+-- tabix_mutect
        'pipette_jobs/tabix_mutect/sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz',
        'pipette_jobs/tabix_mutect/sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz.tbi',
        'pipette_jobs/mutect/sg/gather/sample.call_stats.txt',
        'pipette_jobs/callstats_to_maflite/sample.maf'
        ]
    new_names = [
        'sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz',
        'sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz.tbi',
        'sample.mutect.call_stats.txt',
        'sample.mutect.maflite.txt'
    ]
    make_links(subpaths, new_names)



    subpaths = [
        'pipette_jobs/links_for_broad/mutect_sg_gather/sample.coverage.wig.txt.gz',
        'pipette_jobs/links_for_broad/mutect_sg_gather/sample.power.wig.txt.gz'
        ]
    make_archive(subpaths, 'mutect_intermediates.tar')


if 'M2' in sub_workflow:
    subpaths = [
        'pipette_jobs/tabix_mutect2/sample.broad-mutect2.DATECODE.somatic.vcf.gz',
        'pipette_jobs/tabix_mutect2/sample.broad-mutect2.DATECODE.somatic.vcf.gz.tbi'
        ]
    make_links(subpaths)

    subpaths = [
        'pipette_jobs/M2_scatter/sg/gather/stdout.txt',
        'pipette_jobs/M2_scatter/sg/gather/stderr.txt'
        ]
    make_archive(subpaths, 'm2_intermediates.tar')


if 'variantbam' in sub_workflow:
    subpaths = [
        'pipette_jobs/variant_bam_normal/normal.var.bam',
        'pipette_jobs/variant_bam_tumor/tumor.var.bam'
        ]
    make_links(subpaths)


    subpaths = [
        'pipette_jobs/links_for_broad/variant_bam_normal/merged_rules.bed.gz',
        'pipette_jobs/links_for_broad/variant_bam_normal/qcreport.txt.gz',
        'pipette_jobs/links_for_broad/variant_bam_tumor/merged_rules.bed.gz',
        'pipette_jobs/links_for_broad/variant_bam_tumor/qcreport.txt.gz'
        ]
    make_archive(subpaths, 'variantbam_intermediates.tar')


if 'haplotypecaller' in sub_workflow:
    subpaths = [
        'pipette_jobs/extract_bam_id/upload.txt',
        'pipette_jobs/haplotypecaller_sg/sg/gather/sample.gvcf.gz',
        'pipette_jobs/haplotypecaller_sg/sg/gather/sample.gvcf.gz.tbi'
    ]
    new_names = [
        'bam_id.txt',
        'haplotype_caller.gvcf.gz',
        'haplotype_caller.gvcf.gz.tbi'
    ]
    make_links(subpaths, new_names)


if 'fragcounter' in sub_workflow:

    subpaths = [
        'pipette_jobs/links_for_broad/fragcounter_normal/cov.rds.gz',
        'pipette_jobs/links_for_broad/fragcounter_tumor/cov.rds.gz'
    ]
    new_names = [
        'normal.cov.rds.gz',
        'tumor.cov.rds.gz'
    ]
    make_links(subpaths, new_names)


    subpaths = [
        'pipette_jobs/fragcounter_normal/cov.gc_correction.png',
        'pipette_jobs/fragcounter_normal/cov.map_correction.png',
        'pipette_jobs/fragcounter_normal/cov.og_gc_correction.png',
        'pipette_jobs/fragcounter_normal/stdout.txt',
        'pipette_jobs/fragcounter_normal/stderr.txt',
        'pipette_jobs/fragcounter_tumor/cov.gc_correction.png',
        'pipette_jobs/fragcounter_tumor/cov.map_correction.png',
        'pipette_jobs/fragcounter_tumor/cov.og_gc_correction.png',
        'pipette_jobs/fragcounter_tumor/stdout.txt',
        'pipette_jobs/fragcounter_tumor/stderr.txt',
        ]
    make_archive(subpaths, 'fragcounter_intermediates.tar')


if 'oxoq' in sub_workflow:
    subpaths = [
        'pipette_jobs/oxoq/sample.oxoQ.txt'
        ]
    make_links(subpaths)

    subpaths = [
        'pipette_jobs/oxoq/tumor.oxog_metrics',
        'pipette_jobs/oxoq/stdout.txt',
        'pipette_jobs/oxoq/stderr.txt'
        ]
    make_archive(subpaths, 'oxoq_intermediates.tar')



if 'tokens' in sub_workflow:
    subpaths = [
        'pipette_jobs/links_for_broad/tokens/sample.tok.gz'
        ]
    make_links(subpaths)


if 'hello' in sub_workflow:
    subpaths = [
        'pipette_jobs/hello/outfile.txt'
        ]
    make_links(subpaths)


#########################
# end task-specific calls
run('/opt/src/algutil/monitor_stop.py')
print('\n######\n')
print(os.getcwd())
dirs = sorted(os.listdir( '.' ))
for f1 in dirs:
   print f1

#os.link('../monitor_stop.log','monitor_stop.log')
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

#         #dRanger
#         File dRanger_intermediates_tar="output_files/dRanger_intermediates.tar"
#         File sample_broad_dRanger_DATECODE_somatic_sv_vcf_gz="output_files/sample.broad-dRanger.DATECODE.somatic.sv.vcf.gz"
#         File sample_broad_dRanger_DATECODE_somatic_sv_vcf_gz_tbi="output_files/sample.broad-dRanger.DATECODE.somatic.sv.vcf.gz.tbi"
#         File sample_broad_dRanger_DATECODE_somatic_sv_somatic_sv_detail_txt_gz="output_files/sample.broad-dRanger.DATECODE.somatic.sv.detail.txt.gz"
# 
#         #snowman
#         File snowman_intermediates_tar="output_files/snowman_intermediates.tar"
#         File sample_broad_snowman_DATECODE_somatic_indel_vcf_gz="output_files/sample.broad-snowman.DATECODE.somatic.indel.vcf.gz"
#         File sample_broad_snowman_DATECODE_somatic_indel_vcf_gz_tbi="output_files/sample.broad-snowman.DATECODE.somatic.indel.vcf.gz.tbi"
#         File sample_broad_snowman_DATECODE_germline_indel_vcf_gz="output_files/sample.broad-snowman.DATECODE.germline.indel.vcf.gz"
#         File sample_broad_snowman_DATECODE_germline_indel_vcf_gz_tbi="output_files/sample.broad-snowman.DATECODE.germline.indel.vcf.gz.tbi"
#         File sample_broad_snowman_DATECODE_somatic_sv_vcf_gz="output_files/sample.broad-snowman.DATECODE.somatic.sv.vcf.gz"
#         File sample_broad_snowman_DATECODE_somatic_sv_vcf_gz_tbi="output_files/sample.broad-snowman.DATECODE.somatic.sv.vcf.gz.tbi"
#         File sample_broad_snowman_DATECODE_germline_sv_vcf_gz="output_files/sample.broad-snowman.DATECODE.germline.sv.vcf.gz"
#         File sample_broad_snowman_DATECODE_germline_sv_vcf_gz_tbi="output_files/sample.broad-snowman.DATECODE.germline.sv.vcf.gz.tbi"

#		  merged SV vcf
#         File sample_broad_dRanger_snowman_DATECODE_germline_sv_vcf_gz_tbi="output_files/sample.broad-dRanger_snowman.DATECODE.somatic.sv.vcf.gz"
#         File sample_broad_dRanger_snowman_DATECODE_germline_sv_vcf_gz_tbi="output_files/sample.broad-dRanger_snowman.DATECODE.somatic.sv.vcf.gz.tbi"

        #contest, mutect
        File tumor_bam_contamination_txt_firehose="output_files/tumor.bam.contamination.txt.firehose"
        File contest_intermediates_tar_gz="output_files/contest_intermediates.tar"
        File sample_mutect_call_stats_txt="output_files/sample.mutect.call_stats.txt"
        File sample_mutect_maflite_txt="output_files/sample.mutect.maflite.txt"
        File mutect_intermediates_tar="output_files/mutect_intermediates.tar"
        File sample_broad_mutect_DATECODE_somatic_snv_mnv_vcf_gz="output_files/sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz"
        File sample_broad_mutect_DATECODE_somatic_snv_mnv_vcf_gz_tbi="output_files/sample.broad-mutect.DATECODE.somatic.snv_mnv.vcf.gz.tbi"

        #mutect2
#        File sample_broad_mutect2_DATECODE_somatic_snv_mnv_vcf_gz="output_files/sample.broad-mutect2.DATECODE.somatic.vcf.gz"
#        File sample_broad_mutect2_DATECODE_somatic_snv_mnv_vcf_gz_tbi="output_files/sample.broad-mutect2.DATECODE.somatic.vcf.gz.tbi"

        # minibam
#        File normal_var_bam="output_files/normal.var.bam"
#        File tumor_var_bam="output_files/tumor.var.bam"
#        File variant_bam_intermediates_tar="output_files/variantbam_intermediates.tar"

#         #haplotype caller
#         File bam_id_txt="output_files/bam_id.txt"
#         File haplotype_caller_gvcf_gz="output_files/haplotype_caller.gvcf.gz"
#         File haplotype_caller_gvcf_gz_tbi="output_files/haplotype_caller.gvcf.gz.tbi"

        #force call genotype

        #frag_counter
        File normal_cov_rds_gz="output_files/normal.cov.rds.gz"
        File tumor_cov_rds_gz="output_files/tumor.cov.rds.gz"
        File fragcounter_intermediates_tar="output_files/fragcounter_intermediates.tar"

        #oxoq
        File sample_oxoQ_txt="output_files/sample.oxoQ.txt"
        File oxoq_intermediates_tar="output_files/oxoq_intermediates.tar"

#        #recapseg
#        File sample_normal_uncorrected_target_order_coverage_gz="output_files/sample.normal.uncorrected_target_order.coverage.gz"
#        File sample_tumor_uncorrected_target_order_coverage_gz="output_files/sample.tumor.uncorrected_target_order.coverage.gz"

        #tokens
#        File sample_tok_gz="output_files/sample.tok.gz"

        }

        runtime {

        docker : "docker.io/broadinstitute/pcawg_broad_wgs_variant_callers:latest"
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
