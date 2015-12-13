library(DECIPHER)
library(Biostrings)
library(ShortRead)

crispr_db_spacer_path="spacer_list.txt"
db_spacer=readFasta(crispr_db_spacer_path)
all_spacer_seqs=sread(db_spacer)
db_spacer_ids=id(db_spacer)
spacer_id=as.character(db_spacer_ids)

# parse source data into one-header-one-seq  format
spacers=NULL
for(i in 1:length(spacer_id)){
   temp=unlist(strsplit(spacer_id[i],split="\\|"))
   for(j in 1:length(temp)){
      thisRow=c(temp[j],toString(unlist(all_spacer_seqs[i])))
      spacers=rbind(spacers,thisRow)
   }
   
}

# convert spacers matrix into DNA string set
spacers_dnasets=DNAStringSet(spacers[,2])
names(spacers_dnasets)=spacers[,1]
multi_aligned_result=AlignSeqs(spacers_dnasets,processors=10)
multi_aligned_result=AlignSeqs(spacers_dnasets[1:10000],processors=10)
