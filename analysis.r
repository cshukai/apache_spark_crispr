library(rhdfs)
hdfs.init()

#############################analysis of kmer############################3
d=hdfs.ls("bac_26/10mer/",recurse=T)
all_paths=d[,"file"]
result_paths=all_paths[grep(pattern="dis_group/part",x=all_paths)]
species=NULL
for(i in 1:length(result_paths)){
  tmp=unlist(strsplit(x=result_paths[i],split="/"))
  species=c(species,tmp[6])
}

# use k-mer palindromes to idenitfy crispr array
species=unique(species)
kmer_length=10
spacer_max=100
spacer_min=15
result=list()
counter=1
for(i in 1:length(species)){
 
 related_paths=result_paths[grep(pattern=species[i],x=result_paths)]
 for(j in 1:length(related_paths)){
  print(related_paths[j])
  d=hdfs.read.text.file(related_paths[j])
  if(!is.null(d)){
     for(k in 1:length(d)){
         d_clean=gsub(gsub(x=gsub(x=d[k],pattern="\\(",replacement=""),pattern="\\]\\)",replacement=""),pattern="\\[",replacement="")
         tmp=unlist(strsplit(x=d_clean, split=","))
         junction_dis=tmp[1]
         if(length(tmp)>2){
           
           starts=sort(as.numeric(tmp[2:length(tmp)]))
           dist_between_starts=diff(starts)-kmer_length
           validated_starts=NULL
           for(m in 1:length(dist_between_starts)){
                if(dist_between_starts[m]>spacer_min && dist_between_starts[m]<spacer_max){
                    validated_starts=c(validated_starts,starts[m],starts[m+1])
                }
           }
           if(length(validated_starts)>1){
              result[[counter]]=c(as.numeric(junction_dis),sort(unique(validated_starts)))
              names(result)[counter]=species[i]
              counter=counter+1
           }
         }
     }
  }
 }
} 


#################summarization################################

# identify  perfect palindrome
perfect_palindrome_species=NULL
result_refined=NULL
for(i in 1:length(result)){
    this_structure=result[[i]]
    if(this_structure[1]==0){
    perfect_palindrome_species=c(perfect_palindrome_species,names(result)[i])
    }
    if(this_structure[1]>=50){
    result[[i]]=NULL
    }

}

perfect_palindrome_species=unique(perfect_palindrome_species)


list_labels=names(result)
species_with_putative_CRISPR_array=names(table(list_labels))




for(i in 1:length(species_with_putative_CRISPR_array)){
    this_spec=species_with_putative_CRISPR_array[i]
    target_list_idx=grep(x=list_labels,pattern=this_spec)
    for(j in 1 :length(target_list_idx) ){
       this_target=result[target_list_idx[j]]
       this_jun_dis=this_target[1]
       if(this_jun_dis<50){
          
       }
    }
    
}
save.image("palind_crispr_result.RData")


