
bac_collectoin_home="/home/sc724/data/seq/bacteria/ftp.ensemblgenomes.org/pub/bacteria/release-26/fasta"

#unzip raw data

strain_path=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna"))
gz_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.gz"))
genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa.gz"))
top_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.toplevel.fa.gz"))

for(i in 1:length(genome_paths)){
cmd=paste("gunzip",genome_paths[i],sep=" ")
system(cmd)
}

for(i in 1:length(top_paths)){
cmd=paste("gunzip",top_paths[i],sep=" ")
system(cmd)
}



unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))


#cleanup
setwd("/scratch/shchang/Palindrome/cleanUpData")
for(i in 1:length(unzipped_genome_paths)){
    cmd=paste("./cleanUpFirst",unzipped_genome_paths[i],sep="  ")
    system(cmd)
}

#upload data to hdfs
cleanedDatasets=Sys.glob(file.path("..", "intermediate_data","*.clean"))
for(i in 1:length(cleanedDatasets)){
   tmp=paste("hadoop fs -put",cleanedDatasets[i],sep=" ")
   cmd=paste(tmp,"/sc724",sep=" ")
   cat(cmd,file="upload.sh",append=T,fill=T)
}

save.image("preprocessing.RData")

#generate run script
