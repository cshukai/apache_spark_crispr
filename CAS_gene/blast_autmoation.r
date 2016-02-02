#build_db
all_species=dir()

for(i in  1:length(all_species)){
    tmp=paste('/share/sw/blast/2.2.30+/bin/makeblastdb -in',all_species[i],sep=" ")
    tmp2=paste(tmp,'-dbtype prot -parse_seqids',sep=" ")
    tmp3=paste(all_species[i],"db",sep=".")
    cmd=paste(tmp2,tmp3,sep=" ")
    system(cmd)
}