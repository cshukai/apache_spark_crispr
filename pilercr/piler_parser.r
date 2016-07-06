PILER_home="/home/shchang/scratch/crispr_arr/pilercr"
setwd(PILER_home)
out_files=Sys.glob(file.path("*"))
#parse crt output into tabular format
result=NULL
for(i in 1:length(out_files)){
   thisStrain=out_files[i]
   d=readLines(file(thisStrain))
   if(length(grep(pattern="0 putative",x=d))==0){
      summary_header_idx=grep(pattern="SUMMARY BY POSITION",x=d)
      first_crispr_idx=summary_header_idx+8;
      numOfArray=length(d)-first_crispr_idx+1;
      for(j in 1:numOfArray){
         headerIdx=grep(pattern=paste("Array",j,sep=" "),x=d)
         firstIdx=headerIdx+5
         MeaningFulRowIdx=firstIdx
         is.repeated=T
         while(is.repeated){
             firstIdx=firstIdx+1
             terminateSignal=grep(pattern="=",x=d[firstIdx])
             if(length(terminateSignal)>0){
                 is.repeated=F
             }
             else{
                 MeaningFulRowIdx=c(MeaningFulRowIdx,firstIdx)
             }
             
         }
         for(k in 1:length(MeaningFulRowIdx)){
             thisCRISRinfo=d[MeaningFulRowIdx[k]]
             thisArrNum=j
             tmp=unlist(strsplit(split="  ",x=thisCRISRinfo))
             tmp2=tmp[which(tmp !="")]
             thisUnitStart=as.numeric(tmp2[1])
             thisUnitLen=as.numeric(tmp2[2])
             thisRow=c(thisStrain,thisArrNum,thisUnitStart,thisUnitLen)
             result=rbind(result,thisRow)
         }

         
      }
      
   }
   }
   colnames(result)=c("strain","array_id","unit_loc_start","unit_len")
   write.csv(result,file="../piler.summary.csv",row.names=F)
   save.image("../piler.RData")