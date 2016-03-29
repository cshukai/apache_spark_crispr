################requirment##############
# all raw datasets were unzipped
##########################################

####################pipeline###################
#collecting all fasta files as input
bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))

#generating the shell script to run CRT
CRT_path= "/home/shchang/sw/crispr/CRT1.2-CLI.jar crt"
tmp=paste("java -cp",CRT_path,sep="  ")
CRT_option="-minRL 15 -maxRL 70 -minSL 15  -maxSL 90 -searchWL 6" #ref:http://www.room220.com/crt/cli.html
crt_prefix=paste(tmp,CRT_option,sep="  ") 
out_path="/home/shchang/scratch/crispr_arr/crt/runCRT.sh"
for(i in 1:length(unzipped_genome_paths)){
    this_input_path=unzipped_genome_paths[i]
    tmp2=unlist(strsplit(split="/",x=this_input_path))
    this_outputFileName=tmp2[length(tmp2)]
    this_outputTmp=unlist(strsplit(split="\\.",x=this_outputFileName))
    this_output=this_outputTmp[1]
    cmd_prefix=paste(crt_prefix,this_input_path,sep="  ")
    cmd=paste(cmd_prefix,this_output,sep=" ")
    cat(cmd,file=out_path,append=T,fill=T)
}
##################################################
save.image("crt.RData")