import sys
delete_cols=set(["Match_Norm_Seq_Allele1","Match_Norm_Seq_Allele2","Tumor_Validation_Allele1","Tumor_Validation_Allele2","Match_Norm_Validation_Allele1","Match_Norm_Validation_Allele2","Verification_Status","Validation_Status","Mutation_Status","Sequencing_Phase","Sequence_Source","Validation_Method","Score","BAM_file","Sequencer","Genome_Change","Annotation_Transcript","Transcript_Strand","Transcript_Exon","Transcript_Position","cDNA_Change","Codon_Change","Protein_Change","Other_Transcripts","Refseq_mRNA_Id","Refseq_prot_Id","SwissProt_acc_Id","SwissProt_entry_Id","Description","UniProt_AApos","UniProt_Region","UniProt_Site","UniProt_Natural_Variations","UniProt_Experimental_Info","GO_Biological_Process","GO_Cellular_Component","GO_Molecular_Function","COSMIC_overlapping_mutations","COSMIC_fusion_genes","COSMIC_tissue_types_affected","COSMIC_total_alterations_in_gene","Tumorscape_Amplification_Peaks","Tumorscape_Deletion_Peaks","TCGAscape_Amplification_Peaks","TCGAscape_Deletion_Peaks","DrugBank","gc_content","CCLE_ONCOMAP_overlapping_mutations","CCLE_ONCOMAP_total_mutations_in_gene","CGC_Mutation_Type","CGC_Translocation_Partner","CGC_Tumor_Types_Somatic","CGC_Tumor_Types_Germline","CGC_Other_Diseases","DNARepairGenes_Role","FamilialCancerDatabase_Syndromes","MUTSIG_Published_Results","OREGANNO_ID","OREGANNO_Values","judgement"])
with open(sys.argv[2]) as maf, open(sys.argv[1]+".reduced","w") as outmaf:
    header=maf.readline()
    while header[0] == "#" or not header.strip():
        outmaf.write(header)
        header=maf.readline()
    header=header.strip("\n").split("\t")
    cols_del_inx=set()
    for i,field in enumerate(header):
        if field in delete_cols:
            cols_del_inx.add(i)
        else:
            outmaf.write(field+"\t")
    outmaf.write("\n")

    var_type_i=header.index("Variant_Type")
    ref_i=header.index("Reference_Allele")
    alt_i=header.index("Tumor_Seq_Allele2")

    for line in maf:
        vals=line.strip("\n").split("\t")
        for i,field in enumerate(vals):
            if i in cols_del_inx:
                pass
            elif i == var_type_i:
                if "-" in vals[alt_i] or "-" in vals[ref_i]:
                    outmaf.write(field+"\t")
                else:
                    outmaf.write("SNP"+"\t")
            else:
                outmaf.write(field+"\t")
        outmaf.write("\n")



