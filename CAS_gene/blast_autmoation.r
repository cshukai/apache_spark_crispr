#build_db
all_species=dir()

for(i in  1:length(all_species)){
    tmp=paste('/share/sw/blast/2.2.30+/bin/makeblastdb -in',all_species[i],sep=" ")
    tmp2=paste(tmp,'-dbtype prot -parse_seqids -out ',sep=" ")
    tmp3=paste(all_species[i],"db",sep=".")
    cmd=paste(tmp2,tmp3,sep=" ")
    system(cmd)
}

dir.create("data")
# generate folders
for(i in  1:length(all_species)){
    allFiles=Sys.glob(file.path(paste(all_species[i],"*",sep="")))
    dest=paste("data/",all_species[i],sep="")
    dir.create(dest)
    for(j in 1:length(allFiles)){
        file.copy(from=allFiles[j],to=dest)
    }

}

save.image("crispr_without_analysis.RData")

##fetch corresponding cas proteins
library(Biostrings)
ref_aa=readAAStringSet("crispr_protein.fasta",format="fasta")
ref_id=names(ref_aa)