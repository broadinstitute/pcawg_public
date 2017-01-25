#!/usr/bin/perl -w
use strict;

my ($libdir) = @ARGV;

# run the gather
my $cmd = "python ${libdir}/gatk-gather.py "  . join(" ", @ARGV);
system($cmd) == 0 or die();

# GZip any WIGGLE files
 system("gzip *.wig.txt");
