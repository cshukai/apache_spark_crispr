#usage: perl newshift.pl <filename> <shift num>
my $myfile = $ARGV[0];
#print $myfile;

open FILE, $myfile or die "Couldn't open file: $!"; 
while (<FILE>){
 $string .= $_;
}
close FILE;


my $linelen = $ARGV[1];

my @array = $string =~ /(.{$linelen})/g;
print join ("\n", @array);
