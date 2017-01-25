import os
import sys
import collections

#this should handled via installing the package in docker
#sys.path.append('/home/unix/gsaksena/CancerGenomeAnalysis/trunk/Python/pipette/python_client')
#import pipetteClient

from pipette.python_client import PipetteClient

def run_pcawg_pipeline(communicationDirBase, pipelineOutDir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam_tumor, bam_normal):
        
    pipeline = initialize_pipeline(communicationDirBase, pipelineOutDir)
    annotations = {}
    
    ############ MODIFY THESE LINES AS NEEDED FOR YOUR TOOL #####################
    module_subdir = 'variant_bam_tumor'
    variant_bam_tumor_outputs = variant_bam_pm(pipeline, module_subdir, bam_tumor, adaptor_dir, module_dir, refdata_dir)
    module_subdir = 'variant_bam_normal'
    variant_bam_normal_outputs = variant_bam_pm(pipeline, module_subdir, bam_normal, adaptor_dir, module_dir, refdata_dir, )
    #############################################################################
    
    pipeline.go()

    #add to annotation variable
    #dump annotation variable to disk

####################
## Write a custom functoin that looks something like this.
## bam - a parameter that I need
## everything else - keep the same
def variant_bam_pm(pipeline, module_subdir, bam, adaptor_dir, module_dir, refdata_dir): 

    ################################ CUSTOM FOR VARIANT BAM ##################
    rule = 'rules.txt'
    bam_filename = os.path.basename(bam)
    bam_basename = bam_filename[:-4]
    bam_outname = bam_basename + '.var.bam'
    module_libdir = os.path.join(module_dir,'VariantBam') ## replace VariantBam with your task folder name
    rule_file = os.path.join(module_dir, 'VariantBam', rule) 

    ld_path = "LD_LIBRARY_PATH=/cga/fh/pcawg_pipeline/modules/VariantBam/bamtools-2.1.0/lib; echo $LD_LIBRARY_PATH; "
    cmdStr = ld_path
    cmdStr += os.path.join(module_dir,'VariantBam','variant')
    cmdStr += ' '.join([' -i',bam,'-f',rule_file,'-o',bam_outname])
    
    module_variant_bam_output = '$MODULEOUTDIR/' + bam_basename + '.var.bam'
    module_variant_bam_output_bai = module_variant_bam_output + '.bai'
    module_qc_stats = '$MODULEOUTDIR/qcreport.txt'
    module_merged_callstats = '$MODULEOUTDIR/merged_rules.bed'
    ######################################################
    
    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':14400};
    jobName = 'variant_bam'
    ###################### INPUT FILES
    inputFiles = [bam]
    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [module_variant_bam_output, module_variant_bam_output_bai, module_qc_stats, module_merged_callstats]
    filesToBeDeleted = []
    
    filesToBeOutput_expanded = pipeline.dispense(
        moduleSubDir = moduleSubDir, #unique subdirectory under PIPELINEOUTDIR
        cmdStr = cmdStr, #command-line to run
        resources = resources, #dict with maxmem (in GB) and maxtime (in secs)
        jobName= jobName, #non-unique job-type name
        inputFiles=inputFiles, #list of full paths
        filesToBeOutput=filesToBeOutput, #list of full paths, using MODULEOUTDIR
        filesToBeDeleted=filesToBeDeleted, #list of full paths
        caching='PipelineDefault', #True, False, PipelineDefault
        cleanUpJobFilesOnFail='PipelineDefault', #True, False, PipelineDefault
        cleanUpPipelineJobsOnFail='PipelineDefault', #True, False, PipelineDefault
        executionEngine='PipelineDefault' #lsf, local, mockPrint, PipelineDefault
    )
    
    #[module_variant_bam_output, module_variant_bam_output_bai, module_qc_stats, module_merged_callstats] = filesToBeOutput_expanded
    return filesToBeOutput_expanded

def initialize_pipeline(communicationDirBase, pipelineOutDir):
    
    pipeline = PipetteClient(
    #pipeline = pipetteClient.PipetteClient(
        pipelineOutDir, #root dir for pipeline output, must contain the word Pipette somewhere
        pipelineName='pcawg_pipeline', #non-unique pipeline name
        defaultCaching='True',  #True, False
        defaultCleanUpJobFilesOnFail='False', #True, False
        defaultCleanUpPipelineJobsOnFail='False', #True, False
        defaultExecutionEngine='local', #lsf, local, mockPrint
        pipelinePriority=50,  #integer 0-100
        injectionMap={}, #substitution dict for dispense parameters, plus MODULEOUTDIR and PIPELINEOUTDIR
        communicationDirBase=communicationDirBase #must match corresponding Pipette Server
        )
    return pipeline

if __name__ == '__main__':
    if len(sys.argv)>1:
        communicationDirBase = sys.argv[1]
    else:
        communicationDirBase = None
    if len(sys.argv)>2:
        pipelineOutDir = sys.argv[2]
    else:
        pipelineOutDir = os.path.join(os.getcwd(),'pipette_outdir')

    ############### THESE ARE HARDCODED PATHS FOR TESTING, will be removed later
    indiv_id = '9494d409-f6a3-4b72-98e5-972572c56396'
    bam_tumor = '/cgaext/tcga/cghub_store/94/9494d409-f6a3-4b72-98e5-972572c56396/C239.TCGA-13-0720-01A-01W.4.bam'
    bam_normal = '/cgaext/tcga/cghub_store/e8/e83cfed5-34ad-498f-a463-bcad3952f0fe/C239.TCGA-13-0720-10B-01W.4.bam' 
    pipelineOutDir = '/cga/fh/pcawg_pipeline/jobResults_pipette/pcawg/test_workspace2'
    adaptor_dir = '/cga/fh/pcawg_pipeline/utils/firehose_module_adaptor'
    module_dir = '/cga/fh/pcawg_pipeline/modules'
    refdata_dir = '/cga/fh/pcawg_pipeline/refdata'

    ###### this will launch your script for reception by the pipette server
    run_pcawg_pipeline(communicationDirBase, pipelineOutDir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam_tumor, bam_normal)
