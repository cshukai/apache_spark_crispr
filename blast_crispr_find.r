library(Biostrings)
library(ShortRead)

##processing and analysis of crispr db result##
crispr_db_repeat_path="repeat_list.fa"
db_repeat=readFasta(crispr_db_repeat_path)

#analysis of repeat length
repeat_lens=width(sread(db_repeat))
png("boxplot")
 boxplot(repeat_lens)
dev.off()

# analysis of imperfect palindromic structure inside repeat
all_repeat_seqs=sread(db_repeat)
min_arm_len=4
max_arm_len=20
ips_arm_len_cum=NULL
for(i in 1:length(all_repeat_seqs)){
    d=unlist(all_repeat_seqs[i])
    lens=NULL
    for(j in min_arm_len:max_arm_len){
        palind=findComplementedPalindromes(d,max.looplength=43,min.armlength =j)
        palind_num=length(unlist(strsplit(toString(palind),split=",")))
        lens=c(lens,palind_num)
    }
    
}

#length distrubtion
#
#pairwiseAlignment(pattern = , subject = "suuuuucdfasdfdsfsaeexxexxd",type="local")
#http://stackoverflow.com/questions/9328519/how-to-read-from-multiple-fasta-files-with-r