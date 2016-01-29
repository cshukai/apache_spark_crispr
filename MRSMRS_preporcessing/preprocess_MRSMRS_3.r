
bac_collectoin_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
MRSMRS_perscript_home="/home/shchang/sw/MRSRMSR/perlscript"

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

fa_upper_folder=NULL
for(i in 1:length(unzipped_genome_paths)){
  fa_upper_folder=c(fa_upper_folder,unlist(strsplit(unzipped_genome_paths[i],split="dna"))[1])

}


 # generation single line fasta files
 pwd=getwd()
 setwd(MRSMRS_perscript_home)
 for(i in 1:length(unzipped_genome_paths)){
   cmd=paste("sh /home/shchang/sw/MRSRMSR/perlscript/runmerge.sh ", unzipped_genome_paths[i],sep=" ")
   system(cmd)
 }
 setwd(pwd)

single_line_genomes=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa.txt"))
for(i in 1:length(single_line_genomes)){
    file.copy(from=single_line_genomes[i],to=getwd())
}
save.image("MRSMRS_preprocessing.RData")