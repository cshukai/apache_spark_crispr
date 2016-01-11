library(Biostrings)
ref_aa=readAAStringSet("crispr_protein.fasta",format="fasta")
ref_id=names(ref_aa)

target_species=c("streptococcus thermophilus")
target_names=c("streptococcus_thermophilus")

#fetch type- and species-specific protein sequence
for(i in 1:length(target_species)){

dir.create(target_names[i])

temp_cas1_idx=intersect(grep(pattern="cas1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))

cas2_idx=intersect(grep(pattern="cas2",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas2_idx)>0){
 cas2_result=ref_aa[cas2_idx]
 result_path=paste(target_names,"cas2.ref.fasta",sep="/")
 writeXStringSet(cas2_result,append=T,filepath=result_path)
}

cas3_idx=intersect(grep(pattern="cas3",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cas5_idx=intersect(grep(pattern="cas5",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))

cas6_idx=intersect(grep(pattern="cas6",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cas7_idx=intersect(grep(pattern="cas7",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cas9_idx=intersect(grep(pattern="cas9",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cas10_idx=intersect(grep(pattern="cas10",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cas1_idx=setdiff(temp_cas1_idx,cas10_idx)

csf1_idx=intersect(grep(pattern="csf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
cpf1_idx=intersect(grep(pattern="cpf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
c2c1_idx=intersect(grep(pattern="c2c1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
c2c2_idx=intersect(grep(pattern="c2c2",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))



}



save.image("all_ref_cas.RData")