import sys, os, gzip, subprocess

var_str = ""
pass_fail_maf = sys.argv[1]
vcfs = sys.argv[2:]

failed_sites = set()
with open(pass_fail_maf) as maf:
	header = maf.readline()
	while header[0] == "#" or not header.strip():
		header = maf.readline()
	vals = header.strip("\n").split("\t")
	chr_i = vals.index("Chromosome")
	pos_i = vals.index("Start_position")
	ref_i = vals.index("Reference_Allele")
	alt_i = vals.index("Tumor_Seq_Allele2")
	oxoG_i = vals.index("oxoGCut")

	for line in maf:
		vals = line.strip("\n").split("\t")
		if vals[oxoG_i] == "1":
			failed_sites.add(":".join([vals[chr_i], vals[pos_i], vals[ref_i], vals[alt_i]]))
		continue

for i, vcf_filen in enumerate(vcfs):

	vcf = gzip.open(vcf_filen)

	new_vcf = open(vcf_filen.rpartition("/")[-1].rpartition(".vcf")[0] + ".oxoG.vcf", "w")
	line = vcf.readline()
	while line[0] == "#" or not line.strip():
		header = line
		line = vcf.readline()
		new_vcf.write(header)

	vals = line.strip("\n").split("\t")

	contig = vals[0].upper() if vals[0].lower() != "hs37d5" else vals[0]
	contig = contig if contig != "M" else "MT"

	pos = vals[1]
	ref = vals[3]
	alt = vals[4]
	vals[7]=vals[7].strip(";OXOG_Fail=True")

	if "," in alt:
		fail = False
		for alt_s in alt.split(","):
			if ":".join([contig, pos, ref, alt_s]) in failed_sites:
				fail = True
			else:
				continue
		if fail:
			vals[7] += ";OXOG_Fail=True"
			new_vcf.write("\t".join(vals) + "\n")
		else:
			new_vcf.write("\t".join(vals) + "\n")

	else:
		if ":".join([contig, pos, ref, alt]) in failed_sites:
			vals[7] += ";OXOG_Fail=True"
			new_vcf.write("\t".join(vals) + "\n")
		else:
			new_vcf.write("\t".join(vals) + "\n")

	for line in vcf:
		vals = line.strip("\n").split("\t")
		contig = vals[0].upper() if vals[0].lower() != "hs37d5" else vals[0]
		contig = contig if contig != "M" else "MT"

		pos = vals[1]
		ref = vals[3]
		alt = vals[4]
		vals[7]=vals[7].strip(";OXOG_Fail=True")

		if "," in alt:
			fail = False
			for alt_s in alt.split(","):
				if ":".join([contig, pos, ref, alt_s]) in failed_sites:
					fail = True
				else:
					continue
			if fail:
				vals[7] += ";OXOG_Fail=True"
				new_vcf.write("\t".join(vals) + "\n")
			else:
				new_vcf.write("\t".join(vals) + "\n")

		else:
			if ":".join([contig, pos, ref, alt]) in failed_sites:
				vals[7] += ";OXOG_Fail=True"
				new_vcf.write("\t".join(vals) + "\n")
			else:
				new_vcf.write("\t".join(vals) + "\n")
	vcf.close()
	new_vcf.close()

	subprocess.call(["bgzip", vcf_filen.rpartition("/")[-1].rpartition(".vcf")[0] + ".oxoG.vcf"])
	subprocess.call(["tabix", vcf_filen.rpartition("/")[-1].rpartition(".vcf")[0] + ".oxoG.vcf.gz"])
