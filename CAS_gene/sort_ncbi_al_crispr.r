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
if(length(cas3_idx)>0){
 cas3_result=ref_aa[cas3_idx]
 result_path=paste(target_names,"cas3.ref.fasta",sep="/")
 writeXStringSet(cas3_result,append=T,filepath=result_path)
}

cas5_idx=intersect(grep(pattern="cas5",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas5_idx)>0){
 cas5_result=ref_aa[cas5_idx]
 result_path=paste(target_names,"cas5.ref.fasta",sep="/")
 writeXStringSet(cas5_result,append=T,filepath=result_path)
}

cas6_idx=intersect(grep(pattern="cas6",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas6_idx)>0){
 cas6_result=ref_aa[cas6_idx]
 result_path=paste(target_names,"cas6.ref.fasta",sep="/")
 writeXStringSet(cas6_result,append=T,filepath=result_path)
}

cas7_idx=intersect(grep(pattern="cas7",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas7_idx)>0){
 cas7_result=ref_aa[cas7_idx]
 result_path=paste(target_names,"cas7.ref.fasta",sep="/")
 writeXStringSet(cas7_result,append=T,filepath=result_path)
}


cas9_idx=intersect(grep(pattern="cas9",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas9_idx)>0){
 cas9_result=ref_aa[cas9_idx]
 result_path=paste(target_names,"cas9.ref.fasta",sep="/")
 writeXStringSet(cas9_result,append=T,filepath=result_path)
}


cas10_idx=intersect(grep(pattern="cas10",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas10_idx)>0){
 cas10_result=ref_aa[cas10_idx]
 result_path=paste(target_names,"cas10.ref.fasta",sep="/")
 writeXStringSet(cas10_result,append=T,filepath=result_path)
}

cas1_idx=setdiff(temp_cas1_idx,cas10_idx)
if(length(cas1_idx)>0){
 cas1_result=ref_aa[cas1_idx]
 result_path=paste(target_names,"cas1.ref.fasta",sep="/")
 writeXStringSet(cas1_result,append=T,filepath=result_path)
}

csf1_idx=intersect(grep(pattern="csf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(csf1_idx)>0){
 csf1_result=ref_aa[csf1_idx]
 result_path=paste(target_names,"csf1.ref.fasta",sep="/")
 writeXStringSet(csf1_result,append=T,filepath=result_path)
}

cpf1_idx=intersect(grep(pattern="cpf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cpf1_idx)>0){
 cpf1_result=ref_aa[cpf1_idx]
 result_path=paste(target_names,"cpf1.ref.fasta",sep="/")
 writeXStringSet(cpf1_result,append=T,filepath=result_path)
}


c2c1_idx=intersect(grep(pattern="c2c1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(c2c1_idx)>0){
 c2c1_result=ref_aa[c2c1_idx]
 result_path=paste(target_names,"c2c1.ref.fasta",sep="/")
 writeXStringSet(c2c1_result,append=T,filepath=result_path)
}


c2c2_idx=intersect(grep(pattern="c2c2",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(c2c2_idx)>0){
 c2c2_result=ref_aa[c2c2_idx]
 result_path=paste(target_names,"c2c2.ref.fasta",sep="/")
 writeXStringSet(c2c2_result,append=T,filepath=result_path)
}



}



save.image("all_ref_cas.RData")