#usage: sh chname.sh <species folder name> <replace char>

FILES="/home/newdata/$1/dna/hadoop/*"
for f in $FILES
do
    filename=$(basename "$f")
    path=${f%/*}/
    #echo $path
    #echo $filename
    newname=$2${filename#?}
    echo $newname
        mv $f $path$newname
done

