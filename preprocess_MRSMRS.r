bac_collectoin_home="/home/sc724/data/seq/bacteria/ftp.ensemblgenomes.org/pub/bacteria/release-26/fasta"
MRSMRS_perscript_home="/home/sc724/perlscript"

#unzip raw data
strain_path=paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna"))
gz_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.gz"))
genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa.gz"))

save.image("crispr.RData")
