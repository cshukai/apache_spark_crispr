bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))

singleLine_home="/home/shchang/data/singleLineGenome_genome"
single_paths=Sys.glob(file.path(singleLine_home, "*.fa.txt"))

prefix='spark-submit  --class "MRSMRS"   --num-executors 6 target/simple-project-1.0.jar'
for(i in 1:length(unzipped_genome_paths)){
    temp1=unlist(strsplit(x=unzipped_genome_paths[i],split="/"))
    this_genome_id =temp1[length(temp1)]
    thisSingleLine=single_paths[grep(pattern=this_genome_id,x=single_paths,ignore.case=T)]
    if(length(thisSingleLine)!=1){
        print("too many or not found")
    }
    else{
          cmd_1=paste(prefix,this_genome_id,sep="  ")
          cmd_2=paste(cmd_1,thisSingleLine,sep="   ")
          cat(cmd_2,file="run.sh",append=T,fill=T)

    }
    
  
}
    
save.image("process.run.RData")
