################requirment##############
# all raw datasets were unzipped
##########################################

####################pipeline###################
#collecting all fasta files as input
bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))


#generating the shell script to run pilercr
CRT_path= "/home/shchang/sw/crispr/pilercr/1.06/bin/pilercr"
CRT_option=" -noinfo  -minrepeat 15 -maxrepeat 70 -minspacer  15  -maxspacer 90 -minrepeatratio 0.15 -in" 
crt_prefix=paste(CRT_path,CRT_option,sep="  ") 
out_path="/home/shchang/scratch/crispr_arr/pilercr/runPiler.sh"
for(i in 1:length(unzipped_genome_paths)){
    this_input_path=unzipped_genome_paths[i]
    tmp2=unlist(strsplit(split="/",x=this_input_path))
    this_outputFileName=tmp2[length(tmp2)]
    this_outputTmp=unlist(strsplit(split="\\.",x=this_outputFileName))
    this_output=this_outputTmp[1]
    cmd_prefix=paste(crt_prefix,this_input_path,sep="  ")
    cmd_tmp=paste(cmd_prefix,"-out",sep=" ")
    cmd=paste(cmd_tmp,this_output,sep=" ")
    cat(cmd,file=out_path,append=T,fill=T)
}
##################################################
save.image("piler.RData")