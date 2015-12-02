library(snow)

bac_collectoin_home="/home/sc724/data/seq/bacteria/ftp.ensemblgenomes.org/pub/bacteria/release-26/fasta"
MRSMRS_perscript_home="/home/sc724/perlscript"

#unzip raw data

strain_path=paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna"))
gz_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.gz"))
genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa.gz"))
top_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.toplevel.fa.gz"))

for(i in 1:length(genome_paths)){
cmd=paste("gunzip",genome_paths[i],sep=" ")
system(cmd)
}

unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))

fa_upper_folder=NULL
for(i in 1:length(unzipped_genome_paths)){
  fa_upper_folder=c(fa_upper_folder,unlist(strsplit(unzipped_genome_paths[i],split="dna"))[1])

}

# generation of txt1 and txt4 for MRSMRS
for(i in 1:length(fa_upper_folder)){
  cmd=paste("sh /home/sc724/perlscript/runmerge.sh ", fa_upper_folder[i],sep=" ")
  system(cmd)
}

for(i in 1:length(fa_upper_folder)){
  cmd=paste("sh /home/sc724/perlscript/runsplit.sh ", fa_upper_folder[i],sep=" ")
  system(cmd)
}

#upalod to hdfs 
txt1_4_path=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","hadoop","*.txt[14]"))
hdfs_path="bac_26"

for(i in  1:length(txt1_4_path)){
    tmp=paste("hadoop fs -put",txt1_4_path[i],sep=" ")
    cmd=paste(tmp,hdfs_path,sep=" ")
    system(cmd)
}

# formulate the script to run spark-submit
txt_1_path=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","hadoop","*.txt1"))
argu=NULL
for(i in 1:length(txt_1_path)){
 tmp=unlist(strsplit(txt_1_path[i],split="/"))
 tmp2=tmp[length(tmp)]
 tmp3=substr(tmp2,1,nchar(tmp2)-1)
 tmp4=paste(tmp3,"",sep=" ")
 argu=paste(argu,tmp4,sep=" ")
}

tmp5='spark-submit  --class "PalindromeFinder" target/scala-2.10/palindromefinder_2.10-0.1.jar'
cmd=paste(tmp5,argu,sep=" ")
cat(cmd,file="run.sh")
save.image("crispr.RData")






























