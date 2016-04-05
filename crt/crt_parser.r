library("Biostrings")
#assuming current directory is the folder containing all the crt output file
out_files=Sys.glob(file.path("*"))

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
                thisRow=c(thisStrain,thisCRISPRarrID,thisRepeatStartLoc,thisRepeatSeq,thisSpacerSeq)
                result=rbind(result,thisRow)
        
         }
      }
      
   }
  
   
   
    
 
 


}

write.csv(result,"../crt_summary.csv",row.names=F)
save.image("../crt_parser.RData")
