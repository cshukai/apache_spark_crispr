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
targetIdx=NULL
for(i in 1:length(target_species)){
   foundIdx=grep(pattern=target_species[i],x=unzip_gtf_path)
   if(length(foundIdx)==1){
      targetIdx=c(targetIdx,foundIdx)
   }
}

protein_id_list=list()
for(i in 1:length(targetIdx)){
   target_gtf=unzip_gtf_path[targetIdx[i]]
   gtf_tbl=gffRead(target_gtf)
   gtf_tbl_CDS=gtf_tbl[which(gtf_tbl[,"feature"]=="CDS"),]
   target_protein_id=getAttributeField(gtf_tbl_CDS$attributes,field = "protein_id")
   target_protein_id=gsub(pattern="\"",replacement="",x=target_protein_id)
   protein_id_list[[i]]=target_protein_id
}

names(protein_id_list)=target_species
save.image("gtf_protein.RData")
