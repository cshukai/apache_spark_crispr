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

# type I, IV , III , V , VI-1, VI-2
target_species=c("clostridium_kluyveri_dsm_555","acidithiobacillus_ferrooxidans_atcc_23270","staphylococcus_epidermidis_rp62a","francisella_cf_novicida_fx1","alicyclobacillus_acidoterrestris_atcc_49025","listeria_seeligeri_serovar_1_2b_str_slcc3954")
#type I 
#"bacillus_halodurans_c_125"
#"cyanothece_sp_pcc_8802"
target_name=NULL
targetIdx=NULL
for(i in 1:length(target_species)){
   foundIdx=grep(pattern=target_species[i],x=unzip_gtf_path)
   if(length(foundIdx)==1){
      targetIdx=c(targetIdx,foundIdx)
      target_name=c(target_name,target_species[i])
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
   id_set=paste(target_protein_id,collapse=",")
   tmp= paste( "sh epost -db protein -format acc   -id" ,id_set,sep=" ") 
   tmp2=paste(tmp,"sh efetch -format fasta",sep="|")
   file_name=target_name[i]
   cmd=paste(tmp2,file_name,sep=">")
   system(cmd)
}
names(protein_id_list)=target_name
save.image("gtf_protein.RData")
