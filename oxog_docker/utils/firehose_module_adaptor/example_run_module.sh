#!/bin/bash

python run_module.py --cwd /cga/fh/pcawg_pipeline/jobResults_pipette/pcawg/test_workspace/RealignerTargetCreator_normal  --module_libdir /cga/fh/pcawg_pipeline/modules/RealignerTargetCreator --job.count 24 --input.bam /xchip/cga_home/gsaksena/prj/2015/docker_bringup_2015-01-26/target_gen_scatter_gather/input_bams.list --reference.genome /seq/references/Homo_sapiens_assembly19/v1/Homo_sapiens_assembly19.fasta --base.name pairname
