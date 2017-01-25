#!/usr/bin/perl

use strict;

use Getopt::Long;

my $list_out = "";
my $map_out = "";
my $tsv_in = "";
my $picard_out = "";
my $basename = "";
my $map_suffix = "";
my $index = 0;

print "Command line: tsv_to_list.pl ",join (" ",@ARGV),"\n\n";

my @bamfiles;

GetOptions(
           "tsv:s" => \$tsv_in,
           "gatk_bam_list:s" => \$list_out,
           "gatk_map:s" => \$map_out,
           "picard_options:s" => \$picard_out,
           "basename:s" => \$basename,
           "map_modifier:s" => \$map_suffix,
           "map_index" => \$index
           ) or die("Can not parse argument string");


    

if ( $tsv_in eq "" ) {
    die("Input tsv file argument is not specified");
}

if ( $tsv_in =~ /(.*?)\.[bs]am$/ ) {

    die("--base name must be specified if single bam file is passed as --tsv argument") if ( $basename eq "" );

    push @bamfiles, [$basename, $tsv_in];

} else {

    open(TSV, "< $tsv_in" ) or 
        die("Can not open input tsv file $tsv_in: $!");

    while(<TSV>) {
        chomp;
        next if ( /^\s*\r*$/ );

        my @f = split /\t/;

        if ( scalar(@f) < 2 ) {
            die("TSV file has less than two columns in line:\n$_");
        }

        my $id = $f[0];
        my $bam_file = $f[1];
        push @bamfiles, [$id, $bam_file];
    }

    close TSV;
}


if ( $list_out ne "" ) {

    $list_out = "$basename.$list_out" if ( $basename ne "" );

    open(LIST, "> $list_out") or
        die("Can not open output list file $list_out: $!");
}

if ( $map_out ne "" ) {

    if ( $map_suffix eq "" ) {
        die("When --gatk_map option is on, --map_modifier must be provided");
    }

    $map_out = "$basename.$map_out" if ( $basename ne "" );

    open(MAP, "> $map_out") or
        die("Can not open output map file $map_out: $!");
}

if ( $picard_out ne "" ) {

    $picard_out = "$basename.$picard_out" if ( $basename ne "" );

    open(PICARD, "> $picard_out") or
        die("Can not open output picard options file $picard_out: $!");
}

my $i=0;

for my $ref (@bamfiles) {

    $i++;

    my $id = $ref->[0];
    my $bam_file = $ref->[1];

    if ( $list_out ne "" ) {
        print LIST "$bam_file\n";
    }
    if ( $map_out ne "" ) {
        my $bfname = $bam_file;
        if ( $bam_file =~ /\/([^\/]+)$/ ) {
            $bfname = $1;
        }

        if ( $index ) {
            print MAP "$bfname\t$id.$i$map_suffix\n";
        } else {
            print MAP "$bfname\t$id$map_suffix\n";
        }
    }
    if ( $picard_out ne "" ) {
        print PICARD "I=$bam_file";
    }
} 

close LIST if ( $list_out ne "" ) ;
close MAP if ( $map_out ne "" ) ;
close PICARD if ( $picard_out ne "" ) ;


