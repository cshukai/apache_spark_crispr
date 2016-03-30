#assuming current directory is the folder containing all the crt output file
out_files=Sys.glob(file.path("*"))

result=NULL
for(i in 1:length(out_files)){
 thisFileSize=file.size(out_files[i])

   d=readLines(file(out_files[i]))
   if(length(grep(pattern="CRISPR elements were found",x=d))==0){
   
   }
  
   
   
   chr_len=unlist(strsplit(split=": ",x=d[2]))[2]
    
 
 


}
save.image("crt_parser.RData")