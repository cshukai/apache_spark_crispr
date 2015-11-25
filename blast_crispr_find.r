library(Biostrings)
library(ShortRead)

##processing and analysis of crispr db result
crispr_db_repeat_path="/home/shchang/data/crisprdb/repeat_list.fa"
db_repeat=readFasta(crispr_db_repeat_path)
repeat_lens=width(sread(db_repeat))

#pairwiseAlignment(pattern = , subject = "suuuuucdfasdfdsfsaeexxexxd",type="local")
#http://stackoverflow.com/questions/9328519/how-to-read-from-multiple-fasta-files-with-r