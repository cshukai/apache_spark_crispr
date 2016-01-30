library(Biostrings)
library(ShortRead)


# env setup
pwd="/home/shchang/scratch/RepeatFinder"
setwd(pwd)
load("MRSMRS_preprocessing.RData")



#upload to hdfs
for(i in 1:length(unzipped_genome_paths)){
   tmp=paste("hadoop fs -put",unzipped_genome_paths[i],sep=" ")
   cmd=paste(tmp,"/sc724",sep=" ")
   cat(cmd,file="upload.sh",append=T,fill=T)
}

#build hash keys based on length of kmers
k=4


save.image("RepeatFinder.RData")