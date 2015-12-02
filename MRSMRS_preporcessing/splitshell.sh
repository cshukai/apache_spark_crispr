## No shift
num=1
FILES="./*"
for f in $FILES
do
    perl /home/sc724/MRSMRS/perlscript/newshift.pl $f 199 >../hadoop/$f$num
done

## Shift 99
#num=2
#FILES="./*"
#for f in $FILES
#do
#    perl /home/sc724/MRSMRS/perlscript/newshift.pl $f 100 >../hadoop/$f$num
#done

## Shift 179
#num=3
#FILES="./*"
#for f in $FILES
#do
#    perl /home/sc724/MRSMRS/perlscript/newshift.pl $f 20 >../hadoop/$f$num
#done

## shift 189
num=4
FILES="./*"
for f in $FILES
do
    perl /home/sc724/MRSMRS/perlscript/newshift.pl $f 10 >../hadoop/$f$num
done



## shift 198-x+1=149
#num=5
#FILES="./*"
#for f in $FILES
#do
#    perl /home/hc79b/Code/perlscript/newshift.pl $f 50 >../hadoop/$f$num
#done



## New Approach Chunk
#FILES="./*"
#for f in $FILES
#do
#    perl /home/hc79b/Code/perlscript/nonoverlapshift.pl $f 1000 >../hadoop/$f
#done
