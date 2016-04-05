library("Biostrings")
#assuming current directory is the folder containing all the crt output file
out_files=Sys.glob(file.path("*"))


#parse crt output into tabular format
result=NULL
for(i in 1:length(out_files)){
   thisStrain=out_files[i]
   d=readLines(file(thisStrain))
   if(length(grep(pattern="CRISPR elements were found",x=d))==0){
      chr_len=unlist(strsplit(split=": ",x=d[2]))[2]
      cr_record_star_idx=grep(pattern="POSITION",d)+2
      cr_record_end_idx=grep(pattern="Repeats: ",d)-2
      print(thisStrain)
      print(cr_record_star_idx)
      print(cr_record_end_idx)
      for(j in 1:length(cr_record_star_idx)){
         thisCRISPRarrID=j
         
         
         for(k in cr_record_star_idx[j]:cr_record_end_idx[j]){
             thisLine=d[k]
             tmp=unlist(strsplit(x=thisLine,split="\t"))
             thisRepeatStartLoc=tmp[1]
             thisRepeatSeq=tmp[3]
             if(k!=cr_record_end_idx[j]){
            
               thisSpacerSeq=tmp[4]
             } 
          
             else{
               thisSpacerSeq="End"
             }
             
                thisPalinNum=length(findComplementedPalindromes(DNAString(thisRepeatSeq),min.armlength=4,max.looplength=8))
                thisRow=c(thisStrain,thisCRISPRarrID,thisRepeatStartLoc,thisRepeatSeq,thisSpacerSeq,thisPalinNum)
                result=rbind(result,thisRow)
        
         }
      }
      
   }
  
   
   
    
 
 


}
colnames(result)=c("strain","crispr_num","repeat_start","rep_unit","spacer","palin_num")
write.csv(result,"../crt_summary.csv",row.names=F)
save.image("../crt_parser.RData")



# update your R workspace location
#analysis of truncated cases
truncated_result=NULL
strains_with_crispr=names(table(result[,1]))
for(i in 1: length(strains_with_crispr)){
   this_subset=result[which(result[,"strain"] %in% strains_with_crispr[i]),]
   arr_id=names(table(this_subset[,"crispr_num"]))
   for(j in 1:length(arr_id)){
       this_crispr_arr=this_subset[which(this_subset[,"crispr_num"] %in% arr_id[j]),]
       all_repeat_unit=nchar(this_crispr_arr[,"rep_unit"])
       this_range=range(all_repeat_unit)
       max_rep_unit_len=this_range[2]
       min_rep_unit_len=this_range[1]
       if(max_rep_unit_len!=min_rep_unit_len){
          for(k in 1:nrow(this_crispr_arr)){
              this_rep_unit_len=nchar(this_crispr_arr[k,"rep_unit"])
              if(this_rep_unit_len!=max_rep_unit_len) {
                 thisRow=c(strains_with_crispr[i],arr_id[j],k,nrow(this_crispr_arr),this_rep_unit_len,max_rep_unit_len)
                 truncated_result=rbind(truncated_result,thisRow)                  
              }
          }

       }
   }
}


#analysis of sequence variation
seq_var_result=NULL
for(i in 1: length(strains_with_crispr)){
   this_subset=result[which(result[,"strain"] %in% strains_with_crispr[i]),]
   arr_id=names(table(this_subset[,"crispr_num"]))
   for(j in 1:length(arr_id)){
       this_crispr_arr=this_subset[which(this_subset[,"crispr_num"] %in% arr_id[j]),]
       this_rep_unit_len=nchar(this_crispr_arr[1,"rep_unit"])
       seqVarNum=0
       for(k in 1:this_rep_unit_len){
            basePool=NULL
            for(m in 1:nrow(this_crispr_arr)){
              basePool=c(basePool,substr(this_crispr_arr[,"rep_unit"],start=k,stop=k))
            }
            
            if(range(basePool)[1]!=range(basePool)[2]){
                seqVarNum = seqVarNum +1;            
            
            }
            
       }
       
       seqVarRatio= seqVarNum/this_rep_unit_len
       if(seqVarRatio>0){
         thisRow=c(strains_with_crispr[i],arr_id[j],seqVarRatio)
         seq_var_result=rbind(seq_var_result,thisRow)
       }
   }
}
colnames(seq_var_result)=c("strain","array_id","variation_ratio")
write.csv(seq_var_result,file="crt_seq_var.csv",row.names=F)
save.image("crt_parser.RData")


