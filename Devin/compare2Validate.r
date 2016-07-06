home_dir="/home/shchang/scratch/crispr_arr/mrsmrs/classI_result"
paths=Sys.glob(file.path(home_dir, "*"))
species=NULL

# parsing the file structure
for(i in 1:length(paths)){
    tmp=unlist(strsplit(x=paths[i],split="/"))
    this_species_tmp=tmp[length(tmp)]
    tmp2=unlist(strsplit(x=this_species_tmp,split="\\."))
    this_species=tmp2[1]
    print(this_species)
    species=c(species,this_species)# the order of species and paths are the same
}
save.image("../validate.RData")
# comparison
CRT_summary_file="/home/shchang/scratch/crispr_arr/crt"
PILER_summary_file="/home/shchang/scratch/crispr_arr/pilercr"

for(i in 1:length(paths)){
    partFiles=Sys.glob(file.path(paths[i], "crispr_test2","part-*"))
    d=
    
}