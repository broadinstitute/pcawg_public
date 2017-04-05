'''
Created on 5 Apr 2013

@author: stewart

extract oxoQ value from picard oxog_metrics file 
to produce an output file <OUT>/<ID>.oxoQ.txt with the oxoQ value
'''
import sys
import argparse
import csv
import os
import re
from math import log10

if not (sys.version_info[0] == 2  and sys.version_info[1] in [7]):
    raise "Must use Python 2.7.x"

def parseOptions():
    description = '''
    Parse Picard .oxog_metrics file.
    link file to local area
    write oxoG value for CCG context to output file
    '''

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i', '--sample_id', metavar='sample_id', type=str, help='sample_id.',default='')
    parser.add_argument('-m', '--oxog_metrics_file', metavar='oxog_metrics_file', type=str, help='oxog_metrics_file.',default='')
    parser.add_argument('-b', '--bam_file', metavar='bam_file', type=str, help='bam_file.',default='')
    parser.add_argument('-c', '--context', metavar='context', type=str, help='context.', default='CCG')
    parser.add_argument('-o', '--output', metavar='output', type=str, help='output area', default='.')

    args = parser.parse_args()

    return args


class oxoQ():

    def __init__(self, oxoQ_metrics_file,context):

        self.N = 0
     
        oxoFP = file(oxoQ_metrics_file, 'r')

        numComments = 0   
        head=oxoFP.readline()
        while not head.startswith('SAMPLE_ALIAS'):
            head = oxoFP.readline()
            numComments =  numComments +1 
        
        oxoFP.seek(0,0)   
        
        #for i in range(0,numComments):
        for _ in range(numComments):
            oxoFP.readline()
        
        oxoReader = csv.DictReader(oxoFP, delimiter='\t') #,fieldnames=oxoFN)
        self.NTOT=0
        self.NALTOXO=0
        self.NALTNON=0
        self.oxoQ=float('NaN')
        for line in oxoReader:
            CTXT=line['CONTEXT']
            if re.match(context, CTXT):
                self.N = self.N + 1
                self.NTOT=self.NTOT+int(line['TOTAL_BASES'])
                self.NALTOXO=self.NALTOXO+int(line['ALT_OXO_BASES'])
                self.NALTNON=self.NALTNON+int(line['ALT_NONOXO_BASES'])
                self.oxoQ=float(line['OXIDATION_Q'])
              
                
        if self.N>1:
            er=float(max(self.NALTOXO-self.NALTNON,1.0001))/float(self.NTOT)
            self.oxoQ=-10.0*log10(er)
    
       
        oxoFP.close()


if __name__ == "__main__":

    args = parseOptions()
    sample_id = args.sample_id
    bam_file = args.bam_file
    oxog_metrics_file = args.oxog_metrics_file
    context = args.context
    output = args.output
   

    if len(sample_id)<1:
        sample_id = os.path.split(bam_file)[1].replace('.bam','')

    if not os.path.exists(output):
        os.makedirs(output)

    if len(oxog_metrics_file)<1:
        oxog_metrics_file = bam_file.replace('.bam','.oxog_metrics')
    else:
        if len(bam_file)>=1:
            print('use  -b or -m but not both')


    outFile = output +'/'+sample_id+'.oxoQ.txt'
    outFP = file(outFile,'wt')

    Q=oxoQ(oxog_metrics_file,context)

    outFP.write('%.2f'  % Q.oxoQ)

    outFP.close()

    print('done')
