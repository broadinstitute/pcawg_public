#!perl
use strict;

if (scalar(@ARGV) < 5) {
    die("usage: call_stats_to_maflite.pl <input-call-stats> <build> <CLASSIC|FSTAR|ALL> <f-threshold> <triallelic_mode_KEEP_or_REJECT> <output-file> [csv-of-additional-columns-to-include]\n");
}

my $callStats = shift();
my $build = shift();
my $mode = shift();
my $f_threshold = shift();
my $triallelic_mode = shift();
my $output = shift();

unless ($mode eq "CLASSIC" || $mode eq "FSTAR" || $mode eq "ALL") {
    die("mode must be either CLASSIC, FSTAR or ALL.  The supplied value of $mode is not allowed!\n");
}

unless ($triallelic_mode eq "KEEP" || $triallelic_mode eq "REJECT") {
    die("triallelic_mode must be either KEEP or REJECT, found $triallelic_mode\n");
}

my $columnArr = shift() || "";
my @extraColumns = split(",", $columnArr);
chomp(@extraColumns);

my @columns = ('build','chr','start','end','ref_allele','tum_allele1','tum_allele2','tumor_barcode','normal_barcode');
push(@columns, @extraColumns);

my %map = ();
my @parts = ();

open OUT, ">$output" or die $!;
open FILE, $callStats or die $!;
while (my $line = <FILE>) {
    if ($line =~ /#/) { next; }

    chomp($line);
    @parts = split("\t", $line);

    if ($line =~ /contig/) {
        my $count = @parts;
        for(my $i = 0; $i < $count; $i++) {
            $map{$parts[$i]} = $i;
        }

        $map{"chr"} = $map{"contig"};
        $map{"start"} = $map{"position"};
        $map{"end"} = $map{"position"};
        $map{"tum_allele1"} = $map{"ref_allele"};
        $map{"tum_allele2"} = $map{"alt_allele"};
        $map{"tumor_barcode"} = $map{"tumor_name"};
        $map{"normal_barcode"} = $map{"normal_name"};
      



        print OUT join("\t", @columns) . "\n";
        next;
    }

    # revert the tri-allelic flag if requested.  there is no way to do this in mutect directly
    if ($triallelic_mode eq "KEEP") {
        if (get("failure_reasons") eq "triallelic_site") {
            $parts[$map{"failure_reasons"}] = "";
            $parts[$map{"judgement"}] = "KEEP";
        }
    }

    if ($mode eq "CLASSIC") {
        unless(get("judgement") eq "KEEP") { next; }
    }
 
    if ($mode eq "FSTAR") {
        unless(get("judgement") eq "KEEP" || get("judgement") eq "FSTAR-RETAIN") { next; }
    }
    if ($mode eq "ALL") {
        unless(get("judgement") eq "KEEP"||get("judgement") eq "REJECT") { next; }
    }
    my @data = ();
    foreach my $col (@columns) {
        push(@data, get("$col"));
    }
    if ($f_threshold == 0 || ($f_threshold > 0 && get("tumor_f") >= $f_threshold)) { 
        print OUT join("\t", @data) . "\n";
    }

}
close FILE;
close OUT;

sub get {
    my ($key) = @_;
    if ($key eq "build") { return $build; }
    return $parts[$map{$key}];
}

