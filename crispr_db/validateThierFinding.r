## parse into proper format

without=read.table("noCrisprSpecies.txt",quote = "",header=F,sep="|")
temp=tolower(without[,2])
temp2=gsub(pattern=" ",replacement="_",temp)
temp3=gsub(pattern="\'",replacement="",temp2)
temp4=gsub(pattern="\\/",replacement="_",temp3)
temp5=gsub(pattern="-",replacement="_",temp4)
temp6=gsub(pattern="\\.",replacement="",temp5)
temp7=gsub(pattern="\\(",replacement="",temp6)
temp8=gsub(pattern="\\)",replacement="",temp7)

without[,2]=temp8
write.csv(without,"without.csv",row.names=F)

save.image("parse.RData")


##map with ensemble annotation file

bac_collectoin_home="/home/shchang/data/ensemble/bac_r_29/ftp.ensemblgenomes.org/pub/release-29/bacteria/gtf"
unzip_gtf_path=gz_paths=Sys.glob(file.path(bac_collectoin_home, "*","*","*.gtf"))

target_path=NULL
for(i in 1:nrow(without)){
  target_idx=grep(pattern=without[i,2],x=unzip_gtf_path,ignore.case=T)
  if(length(target_idx)){
  target_path=c(target_path,unzip_gtf_path[target_idx])
  }
  
  
}

target_path=unique(target_path) # more than species in without , probably due to multiple strains for one specices

save.image("without.RData")
# move working directory to the home of ncbi e-utilities

library("ballgown")
for(i in 1:length(target_path)){

 gtf_tbl=gffRead(target_path[i])
 gtf_tbl_CDS=gtf_tbl[which(gtf_tbl[,"feature"]=="CDS"),]
 target_protein_id=getAttributeField(gtf_tbl_CDS$attributes,field = "protein_id")
 target_protein_id=gsub(pattern="\"",replacement="",x=target_protein_id)
 id_set=paste(target_protein_id,collapse=",")
 tmp= paste( "sh ../epost -db protein -format acc   -id" ,id_set,sep=" ") 
 tmp2=paste(tmp,"sh ../efetch -format fasta",sep="|")
 tmp3=unlist(strsplit(split="/",target_path[i]))
 file_name=tmp3[length(tmp3)-1]
 cmd=paste(tmp2,file_name,sep=">")
 system(cmd)
}


###############################################mri process on computing node
load("crisprdb_without.RData")
cwd=getwd()


#form right directories
species_names=NULL
for(i in 1:length(target_path)){
 tmp=unlist(strsplit(split="/",target_path[i]))
 species_names=c(species_names,tmp[length(tmp)-1])
 dir.create(tmp[length(tmp)-1])
}

setwd("crisprdb_without")
ref_fasta=Sys.glob(file.path("*.fasta"))

for(i in 1:length(species_names)){
    prefix="/share/sw/blast/2.2.30+/bin/blastp  -db"
    
    these_db=paste(species_names[i],"db",sep=".")
    tmp=paste(prefix,these_db,sep=" ")
    output=paste("-out",paste(cwd,species_names[i],sep="/"),sep=" ")
    for(j in 1:length(ref_fasta)){
      tmp2=paste("-query",ref_fasta[j],sep=" ")
      tmp3=paste(tmp,tmp2,sep=" ")
      tmp4=paste(output,ref_fasta[j],sep="/")
      tmp5=paste(tmp3,tmp4,sep=" ")
      cmd=paste(tmp5, "-outfmt 6", sep=" ")
      cat(cmd,file="script.sh",fill=T,append=T)
    }
}



setwd(cwd)
save.image("crisprdb_without.RData")