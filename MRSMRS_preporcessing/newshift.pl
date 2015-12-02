#usage: perl newshift.pl <filename> <shift num>
my $myfile = $ARGV[0];
#print $myfile;

open FILE, $myfile or die "Couldn't open file: $!"; 
while (<FILE>){
 $string .= $_;
}
close FILE;

my $lime = $ARGV[1];
my $linelen = 198;
my $initshift = $linelen-$lime+1;
my $winsize = ($lime-1)*2;



$string =~ s/^.{$initshift}//s;


my @array = $string =~ /(.{$linelen})/g;
print join ("\n", @array);
