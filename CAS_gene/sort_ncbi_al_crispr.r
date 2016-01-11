library(ShortRead)
ref_seq=readFasta("crispr_protein.fasta")
ref_id=as.character(id(ref_seq))


# sort by cas type
cas1_id=ref_id[grep(pattern="cas1",ignore.case=T,x=ref_id)]
cas2_id=ref_id[grep(pattern="cas2",ignore.case=T,x=ref_id)]
cas3_id=ref_id[grep(pattern="cas3",ignore.case=T,x=ref_id)]
cas5_id=ref_id[grep(pattern="cas5",ignore.case=T,x=ref_id)]

cas6_id=ref_id[grep(pattern="cas6",ignore.case=T,x=ref_id)]
cas7_id=ref_id[grep(pattern="cas7",ignore.case=T,x=ref_id)]
cas9_id=ref_id[grep(pattern="cas9",ignore.case=T,x=ref_id)]
cas10_id=ref_id[grep(pattern="cas10",ignore.case=T,x=ref_id)]

csf1_id=ref_id[grep(pattern="csf1",ignore.case=T,x=ref_id)]
cpf1_id=ref_id[grep(pattern="cpf1",ignore.case=T,x=ref_id)]
c2c1_id=ref_id[grep(pattern="c2c1",ignore.case=T,x=ref_id)]
c2c2_id=ref_id[grep(pattern="c2c2",ignore.case=T,x=ref_id)]


target_species=c("streptococcus thermophilus")

#type II

save.image("all_ref_cas.RData")