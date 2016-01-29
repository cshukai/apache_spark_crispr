cohort_home="/home/sc724/spark/Palindrome-master/10mer"

paths=Sys.glob(file.path(cohort_home, "*","*","*"))


# contruct identifier for each genome
all_species=NULL
all_chromsome=NULL
file_identifier=NULL
for(i in 1:length(paths)){
temp=unlist(strsplit(paths[i],"/"))
all_species=c(all_species,temp[7])
all_chromsome=c(all_chromsome,temp[8])
file_identifier=rbind(file_identifier,c(all_species,all_chromsome))
}

# merge all the par files for each genome
for(i in 1:length(file_identifier)){
  par_idx=

}