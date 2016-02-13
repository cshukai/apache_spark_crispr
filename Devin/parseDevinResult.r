# env setup
library(rhdfs)
hdfs.init()


library(SparkR)
pwd=getwd()
sc=sparkR.init(master="local[4]")
sqlContext=sparkRSQL.init(sc)


###########################extraction of useful repeat pair######

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

# identify  perfect palindrome and way-too-largee jucntion distance
perfect_palindrome_species=NULL
impossible_idx=NULL
for(i in 1:length(result)){
    this_structure=result[[i]]
    if(this_structure[1]==0){
    perfect_palindrome_species=c(perfect_palindrome_species,names(result)[i])
    }
    if(this_structure[1]>=50){
    impossible_idx=c(impossible_idx,i)
    }
}



perfect_palindrome_species=unique(perfect_palindrome_species)

# summary of repeat sequence
species_with_putative_crispr=NULL

for(i in 1:length(result)){
    if(length(which(impossible_idx == i))==0){
        this_list=result[[i]]
        this_spec_name=names(result)[[i]]
        left_arm_positions=sort(this_list[2:length(this_list)])
        for(j in 1:length(left_arm_positions)){
           cat("")
        }
    }
    
    
}
save.image("palind_crispr_result.RData")


