
####################pipeline###################
#collecting all fasta files as input
bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))


#generating scp script
scp_prefix="scp -i c9.private shchang@rc4.rnet.missouri.edu"
script_name="tranfer.sh"
for(i in 1:length(unzipped_genome_paths)){
cmd_tmp=paste(scp_prefix,unzipped_genome_paths[i],sep=":")
cmd=paste(cmd_tmp,".",sep=" ")
cat(cmd,file=script_name,fill=T,append=T)

}
save.image("utility.RData")