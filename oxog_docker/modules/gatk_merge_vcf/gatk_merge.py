import sys
import os
import subprocess

oxoG_vcf=sys.argv[1]
vcfs_and_centers=sys.argv[2:]
vcfs =[]
centers=[]

for vcf in vcfs_and_centers:
    if ".vcf.gz" in vcf:
        vcfs.append(vcf)
    elif vcf != "--merge":
        centers.append(vcf)

var_str=""
id=oxoG_vcf.rpartition("/")[-1].partition(".")[0]
var_str+=" --variant:oxoG_pass " + oxoG_vcf

for i,vcf_filen in enumerate(vcfs):
    if ".gz" not in vcf_filen:
        subprocess.call("bgzip -c "+vcf_filen+" > " + vcf_filen.rpartition("/")[-1]+".gz",shell=True)
        vcf_filen= vcf_filen.rpartition("/")[-1]+".gz"
        subprocess.call(["tabix",vcf_filen])
        vcfs[i]=vcf_filen

    if not os.path.isfile(vcf_filen+".tbi"):
        print "un-indexed vcf! trying to index"
        os.symlink(vcf_filen,vcf_filen.rpartition("/")[-1])
        vcf_filen=vcf_filen.rpartition("/")[-1]
        vcfs[i]=vcf_filen
        subprocess.call(["tabix",vcf_filen])

    var_str+=" --variant:"+centers[i]+" " + vcf_filen

subprocess.call("java -jar /cga/fh/pcawg_pipeline/modules/gatk_merge_vcf/GenomeAnalysisTK.jar -T CombineVariants -R /cga/fh/pcawg_pipeline/refdata/public/human_g1k_v37_decoy.fasta -o "+id+".merged.vcf --genotypemergeoption UNIQUIFY --filteredAreUncalled "+ var_str,shell=True)
