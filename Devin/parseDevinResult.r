##################### env setup############3
library(rhdfs)
hdfs.init()

library(SparkR)
pwd=getwd()
sc=sparkR.init(master="local[4]")
sqlContext=sparkRSQL.init(sc)


#####################user input#################
k=4
spacer_max=65
spacer_min=20
tracr_interval=10
max_loop_len=10
min_loop_len=3
#####################custom function################

getRealNegativePosiiton<-function(positions,k){
    realPositions=-1*(abs(positions)-k)
    return(realPositions)
}


formRepeatPair<-function(item,spacer_max,spacer_min,k,max_loop_len,min_loop_len){
    d_clean=gsub(gsub(x=gsub(x=item,pattern="\\(",replacement=""),pattern="\\]\\)",replacement=""),pattern="\\[",replacement="")
    tmp=unlist(strsplit(x=d_clean, split=","))
    this_seq=tmp[1]
    tmp2=tmp[3:length(tmp)]
    
    if(length(tmp2)>1){
      tmp2[1]=sub(pattern="CompactBuffer",replacement="",tmp2[1])
      tmp2[length(tmp2)]=gsub(pattern="\\)",replacement="",tmp2[length(tmp2)])
      tmp3=sort(as.numeric(tmp2))
      
      pos_positive_strand=tmp3[which(tmp3>0)]
      pos_negative_strand=getRealNegativePosiiton(tmp3[which(tmp3<0)],k)
    #  print(pos_negative_strand)
      pos_flipped_from_negative=abs(pos_negative_strand)
      pos_for_palindromeDetect=sort(c(pos_positive_strand,pos_flipped_from_negative))
     # print(pos_flipped_from_negative)
      distance4RepeatUnit=diff(pos_positive_strand)-1
      distance4StemLoop=diff(pos_for_palindromeDetect)-1
      #print(pos_for_palindromeDetect)
      #print(distance4StemLoop)     
      result=NULL
      for(i in 1:length(distance4RepeatUnit)){
        if(distance4RepeatUnit[i]>spacer_min && distance4RepeatUnit[i]<spacer_max){
            result=rbind(result,c(this_seq,pos_positive_strand[i],pos_positive_strand[i+1]))
        }
      }      
      
      for(i in 1:length(distance4StemLoop)){
        if(distance4StemLoop[i]<=max_loop_len && distance4StemLoop[i]>=min_loop_len){
            guess1=length(grep(pattern=pos_for_palindromeDetect[i],pos_positive_strand))
            guess2=length(grep(pattern=pos_for_palindromeDetect[i+1],pos_positive_strand))
            if(guess2*guess1==0 && (guess2+guess1)>0){
               #print(pos_for_palindromeDetect[i])
               #print(pos_for_palindromeDetect[i+1])
                if(guess1>0){
                   result=rbind(result,c(this_seq,pos_for_palindromeDetect[i],pos_negative_strand[grep(pattern=pos_for_palindromeDetect[i+1],pos_negative_strand)])) 
                       
                }
                else{
                   result=rbind(result,c(this_seq,pos_negative_strand[grep(pattern=pos_for_palindromeDetect[i],pos_negative_strand)],pos_for_palindromeDetect[i+1])) 
                }
            }
            
        }
      }
      
        if(!is.null(result) &&nrow(result)>=1){
            return(result)
        }
    }
    
}



###########################extraction of useful repeat pair######
this_species_result_path="/home/shchang/scratch/crispr_mrsmrs/algorithm_design/4mer/Streptococcus_thermophilus_cnrz1066.GCA_000011845.1.29.dna.chromosome.Chromosome.fa/part-*"
rdd=SparkR:::textFile(sc, this_species_result_path)
 repeat_pair=createDataFrame(sqlContext, SparkR:::flatMap(rdd,formRepeatPair))
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


