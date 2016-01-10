library("ballgown")
bac_collectoin_home="/home/shchang/data/ensemble/bac_r_29/ftp.ensemblgenomes.org/pub/release-29/bacteria/gtf"

#unzip raw annotation data
gz_paths=Sys.glob(file.path(bac_collectoin_home, "*","*","*.gtf.gz"))

for(i in 1:length(gz_paths)){
cmd=paste("gunzip",gz_paths[i],sep=" ")
system(cmd)
}

unzip_gtf_path=gz_paths=Sys.glob(file.path(bac_collectoin_home, "*","*","*.gtf"))

#extract target species 

target_species=c("streptococcus_thermophilus_cnrz")

protein_ids=list()
for(i in 1:length(target_species)){
   target_gtf=unzip_gtf_path[grep(pattern=target_species[i],x=unzip_gtf_path)]
   gtf_tbl=gffRead(target_gtf)
}
save.image("gtf_protein.RData")