library(Biostrings)
library(ShortRead)

##processing and analysis of crispr db result##
crispr_db_repeat_path="repeat_list.fa"
db_repeat=readFasta(crispr_db_repeat_path)

#analysis of repeat
repeat_lens=width(sread(db_repeat))
png("boxplot")
 boxplot(repeat_lens)
dev.off()
#length distrubtion
#
#pairwiseAlignment(pattern = , subject = "suuuuucdfasdfdsfsaeexxexxd",type="local")
#http://stackoverflow.com/questions/9328519/how-to-read-from-multiple-fasta-files-with-r