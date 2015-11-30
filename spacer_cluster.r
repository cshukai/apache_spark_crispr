library('DECIPHER')
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
   
}