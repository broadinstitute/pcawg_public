#!/bin/bash
export aliquot=$1
BAM=$(readlink -f $2)
MAF=$(readlink -f $3)
oxoQ=$4
REFDATA=$5

check_file (){
	if [ -s ${1} ]
	then
		echo "Previous step produced valid output, moving forward." >> $logfile
	else
		echo "Previous step failed to produce nonzero file." >> $logfile
		exit
	fi
}

echo "Start processing ${aliquot}..." > ${aliquot}.oxoG.log
logfile=$(readlink -f ${aliquot}.oxoG.log)

echo "Removing extra columns from MAF" >> $logfile
python /cga/fh/pcawg_pipeline/modules/oxoG/remove_columns/remove_columns.py ${aliquot} ${MAF} >> $logfile 2>&1
check_file ${aliquot}.reduced

echo "Appending oxoQ value to MAF" >> $logfile
python /cga/fh/pcawg_pipeline/modules/oxoG/AppendAnnotation2MAF/AppendAnnotation2MAF.py -i ${aliquot} -m ${aliquot}.reduced -f "picard_oxoQ" -v ${oxoQ} -o . >> $logfile 2>&1
check_file ${aliquot}.picard_oxoQ.maf.annotated

echo "Creating intervals from MAF" >> $logfile
python /cga/fh/pcawg_pipeline/modules/oxoG/createOxoGIntervalList/createOxoGIntervals.py ${MAF} ${aliquot}.oxoG.interval_list >> $logfile 2>&1
check_file ${aliquot}.oxoG.interval_list

echo "Now generating oxoG metrics with GATK" >> $logfile
java -Xmx2g -jar /cga/fh/pcawg_pipeline/modules/oxoG/oxoGMetrics/GenomeAnalysisTK.jar --analysis_type "OxoGMetrics" -R $REFDATA/public/human_g1k_v37_decoy.fasta -I ${BAM} -L ${aliquot}.oxoG.interval_list -o "${aliquot}.oxoG.metrics.txt" >>$logfile 2>&1
check_file ${aliquot}.oxoG.metrics.txt

echo "Now appending oxoG information to the MAF" >> $logfile
python /cga/fh/pcawg_pipeline/modules/oxoG/appendOxoGInfo/filterByReadConfig.py --onlyAddColumnsToCopy ${aliquot}.oxoG.metrics.txt ${aliquot}.picard_oxoQ.maf.annotated ${aliquot}.oxoGInfo.maf.annotated >> $logfile 2>&1
check_file ${aliquot}.oxoGInfo.maf.annotated

echo "Now running oxoG filter" >> $logfile

Xvnc :$$ -depth 16&
XVNC_PID=$!

export DISPLAY=:$$

old_path=$LD_LIBRARY_PATH

export LD_LIBRARY_PATH="/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/runtime/glnxa64:/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/bin/glnxa64:/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/sys/os/glnxa64:/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/sys/java/jre/glnxa64/jre/lib/amd64/native_threads:/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/sys/java/jre/glnxa64/jre/lib/amd64/server:/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/sys/java/jre/glnxa64/jre/lib/amd64"
export XAPPLRESDIR="/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717/X11/app-defaults"

/cga/fh/pcawg_pipeline/modules/oxoG/oxoGFilter_v3/startFilterMAFFile ${aliquot}.oxoGInfo.maf.annotated ${aliquot}.oxoG3.maf.annotated './' '0' '1' '0.96' '0.01' '-1' '36' '1.5' >> $logfile 2>&1

export LD_LIBRARY_PATH=$old_path

kill $XVNC_PID

sh -c "tar cvf ${aliquot}.oxoG.tar  --exclude '*pipette*' *"
