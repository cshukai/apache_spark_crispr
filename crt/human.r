
anno_result=NULL
all_chr=c(1:8,10:12,16:21)
for(k in 1:length(all_chr)){
    this_chr=as.character(k)
    this_crt_file=paste("Homo_sapiens.GRCh38.dna.chromosome.",".fa",sep=as.character(all_chr[k]))
    this_crt=crt_result[which(crt_result[,1] %in% this_crt_file),]
    
    gff_result=result[which(result[,2] %in% this_chr),]
    gffresul_start=as.numeric(as.character(gff_result[,"start"]))
    gffresult_end=as.numeric(as.character(gff_result[,"end"]))
    
    #arr_num=as.numeric(names(table(this_crt[,2])))
    for(i in 1:nrow(this_crt)){
        this_start=as.numeric(this_crt[i,3])
        this_end=this_start+nchar(this_crt[i,4])-1
        for(j in 1:nrow(gff_result)){
            if(this_start>=gffresul_start[j] && this_end<=gffresult_end[j]){
            anno_result=rbind(anno_result,c(gff_result[j,1],this_crt[i,]))
            }
        }
    }
}


anno_gene=unique(anno_result[,1])
save.image("ontology.RData")