output_home="/home/shchang/scratch/cas_blast/streptococcus_thermophilus"
result_paths=Sys.glob(file.path(output_home, "*.out.txt"))

cas_type=NULL
for(i in 1:length(result_paths)){
   tmp=unlist(strsplit(split="/",x=result_paths[i]))
   this_cas_type=tmp[length(tmp)]
   cas_type=c(cas_type, this_cas_type)
}