
bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"

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
unzipped_top_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.toplevel.fa"))
noisyIdx=union(grep(pattern="dna_rm",x=unzipped_top_paths),grep(pattern="dna_sm",x=unzipped_top_paths))
unzipped_top_refined_paths=unzipped_top_paths[-noisyIdx]

#cleanup
setwd("/scratch/shchang/Palindrome/cleanUpData")
for(i in 1:length(unzipped_genome_paths)){
    cmd=paste("./cleanUpFirst",unzipped_genome_paths[i],sep="  ")
    system(cmd)
}


setwd("/scratch/shchang/Palindrome2/cleanUpData")
for(i in 1:length(unzipped_top_refined_paths)){
    cmd=paste("./cleanUpFirst",unzipped_top_refined_paths[i],sep="  ")
    system(cmd)
}


#upload data to hdfs
cleanedDatasets=Sys.glob(file.path("/home/sc724/intermediate_data","*.clean"))
for(i in 1:length(cleanedDatasets)){
   tmp=paste("hadoop fs -put",cleanedDatasets[i],sep=" ")
   cmd=paste(tmp,"intermediate_data",sep=" ")
   cat(cmd,file="upload.sh",append=T,fill=T)
}

#generate run script
hdfs_filenames=NULL
for(i in 1:length(cleanedDatasets)){
this_name=sub(pattern="../",replacement="",cleanedDatasets[i])
hdfs_filenames=c(hdfs_filenames,this_name)

}

#prefix='spark-submit  --class "PalindromeFinder" --master yarn-client --driver-memory 6G  --executor-memory 6G  --num-executors 3 target/scala-2.10/palindromefinder_2.10-0.1.jar'
prefix='spark-submit  --class "PalindromeFinder"   --num-executors 5 target/scala-2.10/palindromefinder_2.10-0.1.jar'
min_repeat_len=15
min_palin_arm=4


    kmer_len=min_palin_arm
    argu=NULL
    argu_2=NULL
    for(i in 1:length(hdfs_filenames)){
        #tmp=paste("/",hdfs_filenames[i],sep="")  #
        tmp=hdfs_filenames[i]
        this_argu=paste(tmp,kmer_len,sep="  ")
        second_argu=paste(tmp,min_repeat_len,sep="  ")
        argu=c(argu,this_argu)
        argu_2=c(argu_2,second_argu)
    }

    for(i in 1:length(argu)){
        cmd=paste(prefix,argu[i],sep=" ")
        cmd_2=paste(prefix,argu_2[i],sep=" ")
        cat(cmd,file="run.sh",append=T,fill=T)
        cat(cmd_2,file="run.sh",append=T,fill=T)

    }



save.image("preprocessing.RData")
