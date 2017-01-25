#!/usr/bin/python

import os
import sys
import collections





import pipetteClient

def run_pcawg_pipeline(communicationDirBase, pipelineOutDir, pipelinePriority, adaptor_dir, module_dir, refdata_dir, tmp_dir, gnos_outdir, broad_outdir, indiv_id, bam_tumor, oxoQ, vcfs, centers,merge):
    
    

    pipeline = initialize_pipeline(communicationDirBase, pipelineOutDir, pipelinePriority)
    
    annotations = pcawg_pipeline_pp(pipeline, adaptor_dir, module_dir, refdata_dir, tmp_dir, gnos_outdir, broad_outdir, indiv_id, bam_tumor, oxoQ, vcfs,centers,merge)
    
    pipeline.go()

    #add to annotation variable
    #dump annotation variable to disk
    
def pcawg_pipeline_pp (pipeline, adaptor_dir, module_dir, refdata_dir, tmp_dir, gnos_outdir, broad_outdir,
                       indiv_id, bam_tumor, oxoQ, vcfs,centers,merge, bam_normal=None):
#OxoG Pipeline
    if True:
        #make intervals
        module_subdir = 'vcf_to_intervals'
        combined_intervals = vcf_to_intervals(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, vcfs)

        #tbd ensure region is set to all for production, not test.
        module_subdir = 'mutect'
        region = 'all'
        (call_stats, coverage) = mutect_sg_pp(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam_tumor, None, None ,region,combined_intervals)

        module_subdir = 'callstats_to_maflite'
        maflite_file = callstats_to_maflite(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, call_stats)

        module_subdir = 'oncotator_intermediate_maf'
        mutect_maf_output = oncotator(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, maflite_file , "maf")

        module_subdir= 'oxoG'
        (oxoG_full_maf,oxoG_maf,oxoG_tar) = oxoG(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam_tumor, mutect_maf_output, oxoQ)

        if merge: #Old merging code commented out.

            module_subdir = 'oncotator_vcf'
            oxoG_vcf_output = oncotator(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, oxoG_maf , "vcf")

            module_subdir = 'vcfs_to_vcf'
            single_vcf = gatk_merge_vcf(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir,oxoG_vcf_output,vcfs,centers)

            module_subdir = 'tabix_mutect'
            tabix_mutect_outputs = tabix_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, single_vcf,
                                            'oxoG.somatic.snv_mnv')

            make_external_links_pm(pipeline,'mutect_tabix_extlinks',module_dir, gnos_outdir, list(tabix_mutect_outputs)+[oxoG_tar])
        else:
            module_subdir = "annotate_failed_sites_to_vcfs"
            out_vcfs=annotate_vcfs_from_maf(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir,oxoG_full_maf,vcfs)
            make_external_links_pm(pipeline,'mutect_tabix_extlinks',module_dir, gnos_outdir, list(out_vcfs)+[oxoG_tar])

    else:

        call_stats = None

#VariantBam "pipeline"
    if False:
        # TBD note new version available from Jeremiah
        module_subdir = 'variant_bam_tumor'
        variant_bam_tumor_outputs = variant_bam_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, bam_tumor, vcfs)
        make_external_links_pm(pipeline,'variant_bam_tumor_extlinks',module_dir, broad_outdir, variant_bam_tumor_outputs )

        module_subdir = 'variant_bam_normal'
        variant_bam_normal_outputs = variant_bam_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, bam_normal, vcfs)
        make_external_links_pm(pipeline,'variant_bam_normal_extlinks',module_dir, broad_outdir, variant_bam_normal_outputs )


    annotations = {}
    return annotations

def gatk_merge_vcf(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, oxoG_vcf,vcfs,centers):

    ################################ CUSTOM FOR VARIANT BAM ##################
    cmdStr = "python " + module_dir + "/gatk_merge_vcf/"+ "gatk_merge.py " + oxoG_vcf
    for vcf in vcfs:
        if vcf is not None:
            cmdStr += " " + vcf
    for center in centers:
        if center is not None:
            cmdStr += " " + center

    id=oxoG_vcf.rpartition("/")[-1].partition(".")[0]

    uniqueintervals = '$MODULEOUTDIR/'+id+'.merged.vcf'
    ######################################################

    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':2000 * 3600};
    jobName = 'vcfmerge'
    ###################### INPUT FILES
    inputFiles = [oxoG_vcf]
    for vcf in vcfs:
        if vcf is not None:
            inputFiles += [vcf]
    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [uniqueintervals]
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

    uniqueintervals = filesToBeOutput_expanded[0]
    return (uniqueintervals)
def oxoG(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam, maf , oxoQ):
    cmdStr = "bash " + module_dir +"/oxoG/" +"run_oxoG.bash " + " ".join([indiv_id,bam,maf,oxoQ,refdata_dir])

    oxoG_full_maf = '$MODULEOUTDIR/'+indiv_id+".oxoG3.maf.annotated.all.maf.annotated"
    oxoGMaf = '$MODULEOUTDIR/'+indiv_id+".oxoG3.maf.annotated"
    oxoGtar = '$MODULEOUTDIR/'+indiv_id+".oxoG.tar"


    ######################################################

    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':10, 'maxtime':2000 * 3600};
    jobName = 'oxog'
    ###################### INPUT FILES
    inputFiles = [bam,maf]
    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [oxoGMaf,oxoGtar,oxoG_full_maf]
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

    oxoGMaf = filesToBeOutput_expanded[0]
    oxoGtar = filesToBeOutput_expanded[1]
    oxoG_full_maf=filesToBeOutput_expanded[2]
    return (oxoG_full_maf,oxoGMaf,oxoGtar)
def variant_bam_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, bam, vcf1,vcf2,vcf3,vcf4):

    #TBD add call_stats to the cmdStr and inputFiles
    ################################ CUSTOM FOR VARIANT BAM ##################
    rule = 'rules.txt'
    bam_filename = os.path.basename(bam)
    bam_basename = bam_filename[:-4]
    bam_outname = bam_basename + '.var.bam'
    module_libdir = os.path.join(module_dir,'VariantBam') ## replace VariantBam with your task folder name
    rule_file = os.path.join(module_libdir, rule) 

    ld_path = 'export LD_LIBRARY_PATH=' + module_libdir + '/bamtools-2.1.0/lib; echo $LD_LIBRARY_PATH; '
    cmdStr = ld_path
    cmdStr += os.path.join(module_dir,'VariantBam','variant')


    cmdStr += ' '.join([' -i',bam,'-o', bam_outname])
    for vcf in [vcf1,vcf2,vcf3,vcf4]:
        if vcf is not None:
            cmdStr += " -l " + vcf

    module_variant_bam_output = '$MODULEOUTDIR/' + bam_basename + '.var.bam'
    #module_variant_bam_output_bai = module_variant_bam_output + '.bai'
    module_qc_stats = '$MODULEOUTDIR/qcreport.txt'
    module_merged_callstats = '$MODULEOUTDIR/merged_rules.bed'
    ######################################################
    
    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':2000 * 3600};
    jobName = 'variant_bam'
    ###################### INPUT FILES
    inputFiles = [bam]
    for vcf in [vcf1,vcf2,vcf3,vcf4]:
        if vcf is not None:
            inputFiles += [vcf]


    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [module_variant_bam_output, module_qc_stats, module_merged_callstats]
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
    
    variant_bam_output = filesToBeOutput_expanded[0]
    qc_stats = filesToBeOutput_expanded[1]
    merged_callstats = filesToBeOutput_expanded[2]
    return (variant_bam_output, qc_stats, merged_callstats)
def vcf_to_intervals(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, vcfs):

    ################################ CUSTOM FOR VARIANT BAM ##################
    cmdStr = "python " + module_dir + "/vcf_to_intervals/"+ "vcf_to_intervals.py "
    for vcf in vcfs:
        if vcf is not None:
            cmdStr += " " + vcf

    uniqueintervals = '$MODULEOUTDIR/unique.intervals'
    ######################################################

    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':2000 * 3600};
    jobName = 'vcftointervals'
    ###################### INPUT FILES
    inputFiles = []
    for vcf in vcfs:
        if vcf is not None:
            inputFiles += [vcf]
    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [uniqueintervals]
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

    uniqueintervals = filesToBeOutput_expanded[0]
    return (uniqueintervals)
def annotate_vcfs_from_maf(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, maf, vcfs):

    ################################ CUSTOM FOR VARIANT BAM ##################
    cmdStr = "python " + module_dir + "/add_filter_original_vcfs/"+ "apply_maf_to_vcf.py "
    cmdStr += maf
    for vcf in vcfs:
        cmdStr += " " + vcf

    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':2000 * 3600};
    jobName = 'apply_maf_to_vcf'

    ###################### INPUT FILES
    filesToBeOutput = []
    inputFiles = [maf]
    for vcf in vcfs:
        if vcf is not None:
            inputFiles += [vcf]
            filesToBeOutput += ['$MODULEOUTDIR/'+vcf.rpartition("/")[-1].rpartition(".vcf.gz")[0]+".oxoG.vcf.gz",'$MODULEOUTDIR/'+vcf.rpartition("/")[-1].rpartition(".vcf.gz")[0]+".oxoG.vcf.gz.tbi"]

    ##### LIST OF FILES TO BE OUTPUT
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

    out_vcfs = filesToBeOutput_expanded
    return (out_vcfs)

def oncotator(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, maflite_file, outformat):
    
    outfile = indiv_id + '.' + outformat

    #building command line string:
    #cmdStr = os.path.join(module_dir,'oncotator','oncotator')
    cmdStr = 'oncotator'
    mode="TCGAMAF" if outformat == "maf" else "VCF"
    cmdStr += ' '.join([' --db-dir',  os.path.join(refdata_dir,'public','oncotator_db'), '-o '+mode, maflite_file, outfile, 'hg19'])

    mutect_vcf = '$MODULEOUTDIR/' + indiv_id + '.' + outformat
    moduleSubDir = module_subdir

    ####################### set the resource requirements here
    resources = {'maxmem':1, 'maxtime':14400};
    jobName = 'oncotator'
    ###################### INPUT FILES
    inputFiles = [maflite_file]
    ##### LIST OF FILES TO BE OUTPUT
    filesToBeOutput = [mutect_vcf]
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
    
    mutect_vcf_output = filesToBeOutput_expanded[0]
    return (mutect_vcf_output)

def callstats_to_maflite(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, call_stats): 

 #calling the module using the firehose names as the parameters:
    module_libdir = os.path.join(module_dir,'callstats_to_maflite')
    cmdStr = os.path.join(adaptor_dir,'run_module.py') #don't change
    cmdStr += ' --module_libdir ' + module_libdir + ' ' #don't change

    #Add in the parameters as the module is called from firehose:
    cmdStr += ' '.join(['--input.call_stats.file', call_stats, 
                        '--genome.build 37',
                        '--mode ALL',
                        '--output.prefix',indiv_id, 
                        '--triallelic_mode_KEEP_or_REJECT REJECT',
                        '--extra.columns tumor_f,init_t_lod,t_lod_fstar,t_alt_count,t_ref_count,judgement',
                        '--f_threshold','0'])

    #change these variables to the firehose parameters. 
    maflite = '$MODULEOUTDIR/' + indiv_id + '.maf' # maflite output file.
        
    moduleSubDir = module_subdir
        
    resources = {'maxmem':1, 'maxtime':14400}; #memory and time limits
    jobName = 'callstats_to_maflite' #job name
    inputFiles = [call_stats] #parameters from the function to be passed in. 
    #change to include the variables you listed above to save from your function:
    filesToBeOutput = [maflite]
    
    #STOP CHANGING HERE
    #########################
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
    
    maflite_file = filesToBeOutput_expanded[0]
    return maflite_file

def extract_bam_id_pp(pipeline, module_subdir, indiv_id, adaptor_dir, module_dir,refdata_dir, bam_normal):

    module_libdir = os.path.join(module_dir, 'extract_bam_id')
    cmdStr = os.path.join(adaptor_dir,'run_module.py') #don't change
    cmdStr += ' --module_libdir ' + module_libdir + ' ' #don't change

    normal_base_name = os.path.basename(bam_normal)
    normal_base_name = normal_base_name[:-4]

    #Add in the parameters as the module is called from firehose:
    cmdStr += ' '.join([#'--sample_id', normal_base_name, 
                        #'--annotation_nam bamfile_id ',
                        '--bam.file', bam_normal,
                        ])

    #change these variables to the firehose parameters. 
    bam_id_file = '$MODULEOUTDIR/upload.txt' #this is a file, not a single value. How pass first line to Haplotypecaller
    moduleSubDir = module_subdir
        
    resources = {'maxmem':1, 'maxtime':14400}; #memory and time limits
    jobName = 'contest' #job name
    inputFiles = [bam_normal] #parameters from the function to be passed in. 
    #change to include the variables you listed above to save from your function:
    filesToBeOutput = [bam_id_file]
    
    #STOP CHANGING HERE
    #########################
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
    
    bam_id_file = filesToBeOutput_expanded[0]
    return bam_id_file

def OxoQ_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir,  indiv_id, bam_tumor, bam_normal):

    #calling the module using the firehose names as the parameters:
    module_libdir = os.path.join(module_dir,'OxoQ')
    cmdStr = 'python ' + os.path.join(adaptor_dir,'run_module.py') #don't change
    cmdStr += ' --module_libdir ' + module_libdir + ' ' #don't change


    #############################################################
    #START CHANGING HERE:
    #Add in the parameters as the module is called from firehose:
    cmdStr += ' '.join(['--id', indiv_id ,
                        '--bam_file', bam_tumor,
                        '--reference_fasta', os.path.join(refdata_dir,'public','human_g1k_v37_decoy.fasta'),
                        '--dbSNP_vcf', os.path.join(refdata_dir,'public','dbsnp_134_b37.leftAligned.vcf'),
                        '--context', 'CCG',
                        '--output','.'])
  

    moduleSubDir = module_subdir


    resources = {'maxmem':3, 'maxtime':40000}; #memory and time limits
    jobName = 'OxoQ_A' #job name
    inputFiles = [bam_tumor] #parameters from the function to be passed in.
    #change to include the variables you listed above to save from your function:
    filesToBeOutput = ['$MODULEOUTDIR/' + indiv_id + '.oxoQ.txt']

    #STOP CHANGING HERE
    #########################
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
    print ('dispense ' + jobName)

    oxoq_file = filesToBeOutput_expanded[0]

    return oxoq_file
    
def mutect_sg_pp(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, bam_tumor, bam_normal, contest_value_file, region, intervals_filename=None):
    scatter_width = 200
    
    #normal_base_name = os.path.basename(bam_normal)
    #normal_base_name = normal_base_name[:-4]
    tumor_base_name = os.path.basename(bam_tumor)
    tumor_base_name = tumor_base_name[:-4]
    ref_seq_fasta = os.path.join(refdata_dir,'public','human_g1k_v37_decoy.fasta')
    dbsnp_vcf = os.path.join(refdata_dir,'public','dbsnp_134_b37.leftAligned.vcf')
    #
    cosmic_vcf = os.path.join(refdata_dir,'public','hg19_cosmic_v54_120711.vcf')
    

    prepare_args = {'scatter.jobs':str(scatter_width),
                    #'normal.name':normal_base_name,
                    #'normal.bam':bam_normal,
                    'tumor.name':tumor_base_name,
                    'tumor.bam':bam_tumor,
                    'reference.sequence.fasta':ref_seq_fasta,
                    #'dbsnp.vcf':dbsnp_vcf,
                    #'cosmic.vcf':cosmic_vcf,
                    'output.prefix':indiv_id,
                    'downsample.to.coverage':'10000',
                    }
    if contest_value_file is not None:
        # use contents of file as an arg, clipping off \n
        prepare_args['fraction.contamination'] = '`cat ' + contest_value_file + ' | awk 1 ORS=" "`'
    else:
        prepare_args['fraction.contamination'] = '0.02'
        
    
    if region == 'test':
        prepare_args['targets.interval.list'] = os.path.join(refdata_dir,'public','test.bed')  #tbd target.interval.list arg needs to be removed for production
    elif region == 'all':
        pass
    elif region == 'force_call':
        prepare_args['optional.parameter.1'] = '--force_output'
    else:
        raise Exception('unrecognized value for region arg: ' + region)


        
    prepare_args['optional.parameter.1'] = '--force_output'

    module_callstats = os.path.join('$MODULEOUTDIR',indiv_id + '.call_stats.txt')
    wig_file = os.path.join('$MODULEOUTDIR',indiv_id + '.coverage.wig.txt.gz')
    
    module_libdir = os.path.join(module_dir,'mutect')
    input_files = [bam_tumor] # input to prepare
    if contest_value_file is not None:
        input_files.append(contest_value_file)
    if intervals_filename is not None:
        input_files.append(intervals_filename)
        prepare_args['targets.interval.list'] = intervals_filename
    else:
        pass
        #commented out for now, default interval list appears broken
        #wgs_interval_list = os.path.join(refdata_dir,'public','mutect_wgs_intervals.interval_list')
        #prepare_args['targets.interval.list'] = wgs_interval_list

    output_files = [module_callstats,wig_file] #output from gather
    sg_module_subdir = os.path.join(module_subdir,'sg')
    
    scatter_maxmem = 4
    gather_maxmem = 2

    expanded_output_files = scatter_gather_pp(pipeline, sg_module_subdir, module_libdir, adaptor_dir, input_files, output_files, prepare_args, scatter_width, scatter_maxmem, gather_maxmem)
    
    call_stats = expanded_output_files[0]
    coverage = expanded_output_files[1]
    return (call_stats,coverage)

def tabix_pm(pipeline, module_subdir, adaptor_dir, module_dir, refdata_dir, indiv_id, vcf_file, filetag):
    #calling the module using the firehose names as the parameters:
    module_libdir = os.path.join(module_dir,'tabix')
    cmdStr = os.path.join(adaptor_dir,'run_module.py') #don't change
    cmdStr += ' --module_libdir ' + module_libdir + ' ' #don't change

    #Add in the parameters as the module is called from firehose:
    cmdStr += ' '.join([
                        '--input_file', vcf_file,
                        '--output_base_name', indiv_id,
                        '--output_extension', '.' + filetag+'.vcf'])

    #change these variables to the firehose parameters.
    vcf_gz = '$MODULEOUTDIR/' + indiv_id + '.' + filetag + '.vcf.gz'
    vcf_gz_tbi = '$MODULEOUTDIR/' + indiv_id + '.' + filetag + '.vcf.gz.tbi'

    moduleSubDir = module_subdir

    resources = {'maxmem':1, 'maxtime':14400}; #memory and time limits
    jobName = 'tabix' #job name
    inputFiles = [vcf_file] #parameters from the function to be passed in.
    #change to include the variables you listed above to save from your function:
    filesToBeOutput = [vcf_gz, vcf_gz_tbi]

    #STOP CHANGING HERE
    #########################
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

    vcf_gz_outfile = filesToBeOutput_expanded[0]
    vcf_gz_tbi_outfile = filesToBeOutput_expanded[1]
    return (vcf_gz_outfile, vcf_gz_tbi_outfile)

def tsvtolist_pm(pipeline, module_subdir,adaptor_dir, module_dir, refdata_dir, indiv_id, input_bams_fn):

    #calling the module using the firehose names as the parameters:
    module_libdir = os.path.join(module_dir,'tsvtolist')
    cmdStr = os.path.join(adaptor_dir,'run_module.py') #don't change
    cmdStr += ' --module_libdir ' + module_libdir + ' ' #don't change


    #Add in the parameters as the module is called from firehose:
    cmdStr += ' '.join(['--tsv.file', input_bams_fn, 
                        '--base.name', indiv_id, 
                        '--map.modifier', '.cleaned.bam', 
                        '--list.extension', 'list', 
                        '--map.extension', 'map', 
                        ])
        
    moduleSubDir = module_subdir
    mapfile_moddir = '$MODULEOUTDIR/'+indiv_id+'.map'
    listfile_moddir = '$MODULEOUTDIR/'+indiv_id+'.list'
    
    resources = {'maxmem':1, 'maxtime':14400}; #memory and time limits
    jobName = 'tsvtolist' #job name
    inputFiles = [input_bams_fn] #parameters from the function to be passed in. 
    #change to include the variables you listed above to save from your function:
    filesToBeOutput = [mapfile_moddir, listfile_moddir]
    
    #STOP CHANGING HERE
    #########################
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
    
    mapfile = filesToBeOutput_expanded[0]
    listfile = filesToBeOutput_expanded[1]
    return (mapfile, listfile)

def list_of_lists_to_file_pm(pipeline,list_to_file_module_subdir,str_list_list,out_fn):
    cmdStr = ''
    
    num_cols = len(str_list_list[0])
    fns = []
    for colnum in range(num_cols):
        fn = 'col' + str(colnum)
        fns.append(fn)
        for s_list in str_list_list:
            cmdStr += 'echo %s >> %s; '%(s_list[colnum],fn)
    cmdStr += 'paste ' + ' '.join(fns) + ' > ' + out_fn
    
    moduleSubDir = list_to_file_module_subdir
    resources = {'maxmem':1, 'maxtime':300};
    jobName = 'list_to_file'
    inputFiles = []
    filesToBeOutput = ['$MODULEOUTDIR/'+out_fn]
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
    print ('dispense ' + jobName)

    outpath = filesToBeOutput_expanded[0]
    return outpath

def list_to_file_pm(pipeline,list_to_file_module_subdir,str_list,fn):
    cmdStr = ''
    for s in str_list:
        cmdStr += 'echo %s >> %s; '%(s,fn)
    moduleSubDir = list_to_file_module_subdir
    resources = {'maxmem':1, 'maxtime':300};
    jobName = 'list_to_file'
    inputFiles = []
    filesToBeOutput = ['$MODULEOUTDIR/'+fn]
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
    print ('dispense ' + jobName)

    outfn = filesToBeOutput_expanded[0]
    return outfn
    
def initialize_pipeline(communicationDirBase, pipelineOutDir, pipelinePriority):
    
    pipeline = pipetteClient.PipetteClient(
        pipelineOutDir, #root dir for pipeline output, must contain the word Pipette somewhere
        pipelineName='pcawg_pipeline', #non-unique pipeline name
        defaultCaching='True',  #True, False
        defaultCleanUpJobFilesOnFail='False', #True, False
        defaultCleanUpPipelineJobsOnFail='False', #True, False
        defaultExecutionEngine='local', #lsf, local, mockPrint
        pipelinePriority=pipelinePriority,  #integer 0-100, higher value = more important
        injectionMap={}, #substitution dict for dispense parameters, plus MODULEOUTDIR and PIPELINEOUTDIR
        communicationDirBase=communicationDirBase #must match corresponding Pipette Server
        )
    return pipeline

def scatter_gather_pp(pipeline, module_subdir, module_libdir, adaptor_dir, input_files, output_files, prepare_args, scatter_width, scatter_maxmem, gather_maxmem):
    
    prepare_module_subdir = os.path.join(module_subdir,'prepare')
    prepare_out_file = sg_prepare_pm(pipeline, prepare_module_subdir, module_libdir, adaptor_dir, input_files, prepare_args)
    
    scatter_stdouts=[]
    for i in range(scatter_width):
        jobNumber = i+1
        scatter_module_subdir = os.path.join(module_subdir,"scatter.%010d" % (jobNumber))
        scatter_stdout = sg_scatter_pm(pipeline, scatter_module_subdir, jobNumber, module_libdir, adaptor_dir, prepare_out_file, scatter_maxmem)
        scatter_stdouts.append(scatter_stdout)
    
    gather_module_subdir = os.path.join(module_subdir,'gather')

    filesToBeOutput_expanded = sg_gather_pm(pipeline, gather_module_subdir, module_libdir, adaptor_dir, prepare_out_file, scatter_stdouts, gather_maxmem, output_files)
    return filesToBeOutput_expanded

def sg_prepare_pm(pipeline, prepare_module_subdir, module_libdir, adaptor_dir, input_files, prepare_args):

    cmdStr = os.path.join(adaptor_dir,'run_sg_prepare.py')
    cmdStr += ' --module_libdir ' + module_libdir 
    for param in prepare_args:
        cmdStr += ' --' + param + ' ' + prepare_args[param]
        
    moduleSubDir = prepare_module_subdir
        
    resources = {'maxmem':1, 'maxtime':300};
    jobName = 'prepare'
    inputFiles = input_files
    filesToBeOutput = ['$MODULEOUTDIR/prepareResults.out']
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
    print ('dispense ' + jobName)

    outfn = filesToBeOutput_expanded[0]
    return outfn

def sg_scatter_pm(pipeline, scatter_module_subdir, jobNumber, module_libdir, adaptor_dir, prepare_out_file, scatter_maxmem):

    cmdStr = os.path.join(adaptor_dir,'run_sg_scatter.py')
    cmdStr += ' --module_libdir ' + module_libdir 
    cmdStr += ' --prepare_outdir ' + os.path.dirname(prepare_out_file) 
    cmdStr += ' --scatter_index ' + str(jobNumber) 
        
    moduleSubDir = scatter_module_subdir
        
    resources = {'maxmem':scatter_maxmem, 'maxtime':14400};
    jobName = 'scatter'
    inputFiles = [prepare_out_file]
    filesToBeOutput = ['$MODULEOUTDIR/stdout.txt']
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
    print ('dispense ' + jobName)

    
    outfn = filesToBeOutput_expanded[0]
    return outfn

def sg_gather_pm(pipeline, gather_module_subdir, module_libdir, adaptor_dir, prepare_out_file, scatter_stdouts, gather_maxmem, output_files):
    
    cmdStr = os.path.join(adaptor_dir,'run_sg_gather.py')
    cmdStr += ' --module_libdir ' + module_libdir 
    cmdStr += ' --prepare_outdir ' + os.path.dirname(prepare_out_file) 
    for scatter_stdout in scatter_stdouts:
        cmdStr += ' --scatter_outdir ' + os.path.dirname(scatter_stdout)
        
    moduleSubDir = gather_module_subdir
        
    resources = {'maxmem':gather_maxmem, 'maxtime':14400};
    jobName = 'gather'
    inputFiles = scatter_stdouts
    filesToBeOutput = output_files
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
    print ('dispense ' + jobName)

    #returns a list of files in the same order as were passed in as output_files.
    return filesToBeOutput_expanded

def make_external_links_pm(pipeline, module_subdir, module_dir, link_dir, data_files):

    module_libdir = os.path.join(module_dir,'make_external_links')

    cmdStr = 'python %s/make_external_links.py $PIPELINEOUTDIR %s '%(module_libdir, link_dir)
    cmdStr += ' '.join(data_files)

    moduleSubDir = module_subdir

    resources = {'maxmem':1, 'maxtime':14400};
    jobName = 'make_external_links'
    inputFiles = data_files
    filesToBeOutput = [] #Nothing listed, to permit the links to land outside the tree
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
    print ('dispense ' + jobName)

    #returns a list of files in the same order as were passed in as output_files.
    return None

if __name__ == '__main__':
    parse_command_line = True

    adaptor_dir = '/cga/fh/pcawg_pipeline/utils/firehose_module_adaptor'
    module_dir = '/cga/fh/pcawg_pipeline/modules'
    refdata_dir = '/cga/fh/pcawg_pipeline/refdata'
    tmp_dir = '/cga/fh/pcawg_pipeline/tmp'
    pipelinePriority = 60
    
    gnos_outdir = '$PIPELINEOUTDIR/links_for_gnos'
    broad_outdir = '$PIPELINEOUTDIR/links_for_broad'
                                
    
    if parse_command_line:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--ref", default="/cga/fh/pcawg_pipeline/refdata")
        parser.add_argument("commDir")
        parser.add_argument("output")
        parser.add_argument("indiv_id")
        parser.add_argument("bam")
        parser.add_argument("oxoQ")
        parser.add_argument("vcfs", nargs="+")
        
        #communicationDirBase = sys.argv[1]
        #pipelineOutDir = sys.argv[2]
        #indiv_id = sys.argv[3]
        #bam_tumor = sys.argv[4]
        #oxoQ = sys.argv[5]
        #vcfs_and_centers = sys.argv[6:]
        args = parser.parse_args()
        communicationDirBase = args.commDir
        pipelineOutDir = args.output
        indiv_id = args.indiv_id
        bam_tumor = args.bam
        oxoQ = args.oxoQ
        vcfs_and_centers = args.vcfs

        refdata_dir = args.ref
        
        vcfs=[]
        centers=[]
        merge = True if "--merge" in vcfs_and_centers else False
        for arg in vcfs_and_centers:
            if arg != "--merge":
                #center,vcf = vcfs_and_centers.split(",")
                if ".vcf.gz" not in arg:
                    #print("Input for center:" + center + " not vcf format!")
                    print("The file privided was not gzipped vcf:" + arg)
                    sys.exit(1)
                    #some sort of error and exit here.
                else:
                    vcfs.append(arg)
                    #centers.append(center)




    else:
        # select test bam file inputs
        bamnum = 3
        if bamnum == 1:
            #1GB non-paired read exome
            indiv_id = '9494d409-f6a3-4b72-98e5-972572c56396'
            bam_tumor = '/cgaext/tcga/cghub_store/94/9494d409-f6a3-4b72-98e5-972572c56396/C239.TCGA-13-0720-01A-01W.4.bam'
            bam_normal = '/cgaext/tcga/cghub_store/e8/e83cfed5-34ad-498f-a463-bcad3952f0fe/C239.TCGA-13-0720-10B-01W.4.bam'
        
        elif bamnum == 2:
            #Minibammer output
            bam_tumor = '/cga/fh/pcawg_pipeline/test_data/PCAWG-BRCA-US-S-TCGA-AN-A0AT-01A-11D-A045-09.var.short.bam'    
            bam_normal = '/cga/fh/pcawg_pipeline/test_data/PCAWG-BRCA-US-S-TCGA-AN-A0AT-10A-01D-A047-09.var.short.bam'  
            indiv_id = 'PCAWG-BRCA-US-S-TCGA-AN-A0AT'
            
        elif bamnum == 3:
            #Broad Prostate exome 5GB
            bam_tumor = '/cga/fh/pcawg_pipeline/test_data/C529.TCGA-VN-A88R-01A-11D-A364-08.1.bam'
            bam_normal = '/cga/fh/pcawg_pipeline/test_data/C529.TCGA-VN-A88R-10B-01D-A362-08.1.bam'
            indiv_id = 'TCGA-VN-A88R'
        elif bamnum == 4:
            #Sarcoma (rearrangement-prone), WGS but only chromosome 22
            indiv_id = 'PCAWG-SARC-US-S-TCGA-IW-A3M4'
            bam_tumor = '/cga/fh/pcawg_pipeline/test_data/PCAWG-SARC-US-S-TCGA-IW-A3M4-Tumor.bam'
            bam_normal = '/cga/fh/pcawg_pipeline/test_data/PCAWG-SARC-US-S-TCGA-IW-A3M4-Normal.bam'
        elif bamnum == 5:
            # full-sized WGS bam
            bam_tumor = '/cga/fh/pcawg_pipeline/test_data/PCAWG.64d83e97-f798-45d1-b9e6-efaa635b4abb.bam'
            bam_normal = '/cga/fh/pcawg_pipeline/test_data/PCAWG.fa4fa49d-6d53-4ffa-9759-ffb884b28d17.bam'
            indiv_id = 'PCAWG-GBM-US-S-TCGA-14-0786'
        # other params    
        pipelineOutDir = '/cga/fh/pcawg_pipeline/jobResults_pipette/pcawg/landkof_v2_test_i/' + indiv_id
        communicationDirBase = None #None = use default: $HOME/pipette_queue


    run_pcawg_pipeline(communicationDirBase, pipelineOutDir, pipelinePriority, adaptor_dir, module_dir, refdata_dir,  tmp_dir, gnos_outdir, broad_outdir, indiv_id, bam_tumor, oxoQ, vcfs ,centers, merge)

