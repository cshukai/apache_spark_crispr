library(Biostrings)
library(ShortRead)
require(ggplot2)

##processing and analysis of crispr db result##
crispr_db_repeat_path="repeat_list.fa"
db_repeat=readFasta(crispr_db_repeat_path)
db_repeat_ids=id(db_repeat)

crispr_db_spacer_path="/home/shchang/data/crisprdb/spacer_list.fa"
db_spacer=readFasta(crispr_db_spacer_path)
db_spacer_ids=id(db_spacer)

target_genomes_path="/home/shchang/data/bacteria_0_collection/methanocaldococcus_jannaschii_dsm_2661/dna/Methanocaldococcus_jannaschii_dsm_2661.GCA_000091665.1.29.dna.chromosome.Chromosome.fa"
target_genome=readFasta(target_genomes_path)
crispr_ref_id="NC_000909"
#alingment between repeats and genomes



#alignment between spacers adn genoms


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
    seq_txt=toString(d)
    lens=NULL
    for(j in min_arm_len:max_arm_len){
        palind=findComplementedPalindromes(d,max.looplength=43,min.armlength =j)
        palind_num=length(unlist(strsplit(toString(palind),split=",")))
        lens=c(lens,palind_num)

    }
    ips_arm_len_cum=rbind(ips_arm_len_cum,c(seq_txt,as.character(lens)))
   
    
}
colnames(ips_arm_len_cum)=c("repeat_seq",as.character(min_arm_len:max_arm_len))


#get exact count rather than cumulative count
arm_len=matrix(0,nrow =nrow(ips_arm_len_cum), ncol =ncol(ips_arm_len_cum)-1)
for(i in 1:ncol(arm_len)){
  arm_len[,i]=as.numeric(ips_arm_len_cum[,i+1])
}
colnames(arm_len)=colnames(ips_arm_len_cum)[2:ncol(ips_arm_len_cum)]

for(i in 1:ncol(arm_len)){
  if(i!=ncol(arm_len)){
    arm_len[,i]=arm_len[,i]-arm_len[,i+1]
  }
}

result_len=cbind(ips_arm_len_cum[,1],arm_len)
colnames(result_len)=c("repeat_seq",as.character(min_arm_len:max_arm_len))

write.csv(result_len,file="result_len.csv",row.names=F)

#pairwiseAlignment(pattern = , subject = "suuuuucdfasdfdsfsaeexxexxd",type="local")
#http://stackoverflow.com/questions/9328519/how-to-read-from-multiple-fasta-files-with-r