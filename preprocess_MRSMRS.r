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


for(i in 1:length(fa_upper_folder)){
  cmd=paste("sh /home/sc724/perlscript/runmerge.sh ", fa_upper_folder[i],sep=" ")
  system(cmd)
}


save.image("crispr.RData")




































