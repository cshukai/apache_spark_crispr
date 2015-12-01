FILES="$1/dna/*.chromosome.Chromosome.fa"
for f in $FILES
do
    sh /home/sc724/perlscript/mergelines.sh $f

done


mkdir $1/dna/txt
mv $1/dna/*.txt   $1/dna/txt
