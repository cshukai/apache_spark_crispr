output_home="/home/shchang/scratch/cas_blast/streptococcus_thermophilus"
result_paths=Sys.glob(file.path(output_home, "*.out.txt"))

cas_type=NULL
for(i in 1:length(result_paths)){
   tmp=unlist(strsplit(split="/",x=result_paths[i]))
   this_cas_type=tmp[length(tmp)]
   cas_type=c(cas_type, this_cas_type)
}

top_match_list=list()
result_statistics=NULL
seq_identity_cuff=90
e_cuff=0.001
for(i in  1: length(cas_type)){
  this_result=read.table(result_paths[i])
  identity_max=max(this_result[,3])
  minimum_e_value=min(this_result[,11])
  max_bit_score=max(this_result[,12])
  result_statistics=rbind(c(cas_type[i],identity_max,max_bit_score,minimum_e_value))
  top_match_list[[i]]=this_result[intersect(which(this_result[,3] >seq_identity_cuff),which(this_result[,11] <e_cuff)),2] # grab the highly similar reference region
}




names(top_match_list)=cas_type
save.image("analysis.RData")