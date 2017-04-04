
# TODO:
# v1.9.2.0 for oncotator
# Pull base image.
FROM ubuntu:14.04

MAINTAINER Gordon Saksena <gsaksena@broadinstitue.org>
ENV TERM=vt100


RUN \
  sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y build-essential && \
  apt-get install -y software-properties-common && \
  apt-get install -y byobu curl git htop man unzip vim wget && \
  rm -rf /var/lib/apt/lists/*



# Set environment variables.
ENV HOME /root
WORKDIR /root



#Install Oracle Java 7
RUN \
  echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java7-installer && \
  rm -rf /var/cache/oracle-jdk7-installer


#  rm -rf /var/lib/apt/lists/* && \

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-7-oracle

RUN echo "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/" | tee -a /etc/apt/sources.list
RUN echo "deb http://us.archive.ubuntu.com/ubuntu trusty main universe" | tee -a /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9

#Load C++ libraries
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get -y update
RUN apt-get -y --force-yes upgrade
RUN apt-get -y dist-upgrade

RUN apt-get install -y samtools libpng-dev

#load Python2&3, and utilities needed for Matlab MCR install
RUN apt-get install -yq unzip wget bc python-dev python-pip python3 python3-pip libxp6 vcftools #git

ADD utils/matlab2012a_mcr/ /opt/matlab2012a_mcr_install
RUN   cd /opt/matlab2012a_mcr_install && \
      ./install -mode silent -agreeToLicense yes && \
      rm -rf /opt/matlab2012a_mcr_install/*

#TODO: Figure out what to delete after matlab has completed.

ENV MCRROOT=/usr/local/MATLAB/MATLAB_Compiler_Runtime/v717
ENV MCRJRE=$MCRROOT/sys/java/jre/glnxa64/jre/lib/amd64
ENV XAPPLRESDIR=$MCRROOT/X11/app-defaults
ENV MCR_LD_LIBRARY_PATH=$MCRROOT/runtime/glnxa64:$MCRROOT/bin/glnxa64:$MCRROOT/sys/os/glnxa64:$MCRJRE/native_threads:$MCRJRE/server:$MCRJRE/client:$MCRJRE

#RUN cd /opt/MATLAB/MATLAB_Compiler_Runtime/v710/runtime/glnxa64; for l in  /opt/MATLAB/MATLAB_Compiler_Runtime/v710/bin/glnxa64/*; do ln -s $l; done


#ENV LD_LIBRARY_PATH=/cga/fh/pcawg_pipeline/modules/VariantBam/bamtools-2.1.0/lib

#will it work to go into a mounted dir?
WORKDIR /cga/fh/pcawg_pipeline

RUN rm -f /root/.scripts/git-prompt.sh

#Set timezone on Docker instance to something other than UCT.
RUN echo "America/New_York" | sudo tee /etc/timezone; dpkg-reconfigure --frontend noninteractive tzdata

RUN pip install numpy ngslib

RUN mkdir -p /opt/pyvcf_install && cd /opt/pyvcf_install && wget --no-check-certificate 'https://github.com/elephanthunter/PyVCF/archive/master.zip' && unzip master.zip && cd PyVCF-master && python setup.py install && cd .. && rm -Rf PyVCF-master && rm -f master.*

#RUN pip install ngslib

RUN mkdir -p /opt/oncotator_install && cd /opt/oncotator_install &&  git clone https://github.com/broadinstitute/oncotator.git && cd oncotator && git checkout master

#force latest numpy version, in spite of Oncotator's requirements for 1.11.0
RUN mv /opt/oncotator_install/oncotator/setup.py /opt/oncotator_install/oncotator/setup_old.py &&  sed  's/==1.11.0//' /opt/oncotator_install/oncotator/setup_old.py > /opt/oncotator_install/oncotator/setup.py

RUN cd /opt/oncotator_install/oncotator && python setup.py install

ENV CACHE_UPDATE 4

#This is nededed for output hack on oxoG filter.
RUN apt-get install -y tightvncserver

#monitor log
RUN apt-get install dstat


ADD modules /cga/fh/pcawg_pipeline/modules
#ENV LD_LIBRARY_PATH=$MCR_LD_LIBRARY_PATH
# prepare for Matlab-based tools to run
#RUN /opt/matlab2009a_mcr_install/extractCTF /cga/fh/pcawg_pipeline/modules/dRangerPreprocess/fh_dRangerPreprocessGather.ctf
#RUN /opt/matlab2009a_mcr_install/extractCTF /cga/fh/pcawg_pipeline/modules/dRangerFinalize/fh_dRangerFinalize.ctf
#RUN /opt/matlab2009a_mcr_install/extractCTF /cga/fh/pcawg_pipeline/modules/dRanger_BreakPointer/fh_BreakPointerScatter.ctf
ENV LD_LIBRARY_PATH=

ADD utils /cga/fh/pcawg_pipeline/utils
RUN mkdir -p /cga/fh/pcawg_pipeline/tmp
RUN chmod go+rwx /cga/fh/pcawg_pipeline/tmp

ADD pipelines /cga/fh/pcawg_pipeline/pipelines

RUN chmod go+r -R /usr/local/lib/python3.4/dist-packages/
RUN chmod go+w /cga/fh/pcawg_pipeline

RUN chmod go+rx /cga/fh/pcawg_pipeline/pipelines/*.py
RUN chmod go+rx /cga/fh/pcawg_pipeline/pipelines/*.bash
RUN chmod go+rx /cga/fh/pcawg_pipeline/utils/firehose_module_adaptor/*.py

RUN chmod go+wrx /home
ENV HOME /home

#lastline
