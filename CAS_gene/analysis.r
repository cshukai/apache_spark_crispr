####################match by sequence similarity###########
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
  result_statistics=rbind(result_statistics,c(cas_type[i],identity_max,max_bit_score,minimum_e_value))
  top_match_list[[i]]=this_result[intersect(which(this_result[,3] >seq_identity_cuff),which(this_result[,11] <e_cuff)),2] # grab the highly similar reference region
}

names(top_match_list)=cas_type
colnames(result_statistics)=c("cas_type","max_seq_identity","max_alignment_score","e-value")
write.csv(result_statistics,file="result.statistics.csv",row.names=F)
names(top_match_list)=cas_type
save.image("analysis.RData")
################match by location #############
match_ref_region=NULL
# using e-utilies to fetch location information of coding region
for(i in 1:length(top_match_list)){
   this_group_result=top_match_list[[i]]
   if(length(this_group_result)>0){
     this_cas_type=names(top_match_list)[i]
     for(j in 1:length(this_group_result)){
         temp=as.character(this_group_result[j])
         temp2=unlist(strsplit(split="\\|",x=temp))
         this_protein_id=temp2[length(temp2)]
         thisRow=c(this_cas_type,this_protein_id)
         match_ref_region=rbind(match_ref_region,thisRow)
         tmp_cmd=paste("perl protein2gene.pl",this_protein_id, sep=" "); 
         cmd=paste(tmp_cmd,this_protein_id,sep=">") # use protien id as filename for subsequent retrieval
         system(cmd)
     }    
   }
}


# parsing the result returned from ncbi
top_match_list_transformed=NULL
for(i in 1:nrow(match_ref_region)){
    thisProtein=match_ref_region[i,2]
    thisNcbiReturn=readLines(file(thisProtein))
    tmp=thisNcbiReturn[grep(pattern="coded",x=thisNcbiReturn)]
    tmp2=unlist(strsplit(x=tmp,split=":"))
    tmp3=unlist(strsplit(x=tmp2[length(tmp2)],split="\\.\\."))
    thisRegionStart=tmp3[1]
    thisRegionEnd=tmp3[2]
    top_match_list_transformed=rbind(top_match_list_transformed,c(match_ref_region[i,],thisRegionStart,thisRegionEnd))     
    
}

# arrange gene architecture by distance
cas= names(table(top_match_list_transformed[,1]))
cas_location_list=list()
for(i in 1:length(cas)){
thisCas=cas[i]
theseLocStarts=sort(unique(top_match_list_transformed[which(top_match_list_transformed[,1]==thisCas ),3 ]))
cas_location_list[[i]]=theseLocStarts
}
names(cas_location_list)=cas
