#!/bin/sh

source /broad/software/scripts/useuse
use Python-2.7 

while getopts "i:m:f:v:o:?" Option
do
    case $Option in
        i    ) ID=$OPTARG;;
        m    ) MAF=$OPTARG;;
        f    ) FLD=$OPTARG;;
        v    ) VAL=$OPTARG;;
        o    ) OUT=$OPTARG;;
        ?    ) echo "Invalid option: -$OPTARG" >&2
               exit 0;;
        *    ) echo ""
               echo "Unimplemented option chosen."
               exit 0;;
    esac
done


echo "ID:               	${ID}"
echo "MAF:             	${MAF}" 
echo "Fieldname:       ${FLD}" 
echo "Value:           	${VAL}" 
echo "Output area:	${OUT}" 


oopt="-o $OUT "
if [[ -z $OUT ]]; then
   echo "no output full path"
   oopt=""
   OUT="."
fi   

# oxog_metric file in output area
OXOQ=${OUT}/${oxoQ}

ls -latr ${OXOQ}

Dir=`dirname $0`

echo ""
echo "Append Annotation to MAF  command line: "
echo "python $Dir/AppendAnnotation2MAF.py -i $ID -m $MAF  -f $FLD -v $VAL $oopt"

python $Dir/AppendAnnotation2MAF.py -i $ID -m $MAF  -f $FLD -v $VAL  $oopt

echo ""
echo "done"

