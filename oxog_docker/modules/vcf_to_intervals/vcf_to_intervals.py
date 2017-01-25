import sys
import gzip

vcfs=sys.argv[1:]
all_muts=set()
for vcf_filen in vcfs:
    vcf=gzip.open(vcf_filen,"rb")
    header=vcf.readline()
    while header[0] == "#" or not header.strip():
        header=vcf.readline()
    vals=header.strip("\n").split("\t")
    contig=vals[0].upper() if vals[0].lower() != "hs37d5" else vals[0]
    pos=vals[1]
    if vals[6] == "PASS":
            if contig != "M":
                all_muts.add(contig+":"+pos)

            else:
                all_muts.add("MT:"+pos)

    for line in vcf:

        vals=line.strip("\n").split("\t")
        if vals[6] != "PASS":
            continue

        contig=vals[0].upper() if vals[0].lower() != "hs37d5" else vals[0]
        pos=vals[1]

        if contig != "M":
            all_muts.add(contig+":"+pos)
        else:
            all_muts.add("MT:"+pos)

    vcf.close()
with open("unique.intervals","w") as intervals_out:
    for mut in all_muts:
        intervals_out.write(mut+"\n")