sed 1d $*>tmp.txt
perl -00 -pe 's/\n//g; s/ $/\n/' tmp.txt> $*.txt
rm -f tmp.txt
