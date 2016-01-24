library(Biostrings)
ref_aa=readAAStringSet("crispr_protein.fasta",format="fasta")
ref_id=names(ref_aa)

target_species=c("clostridium kluyveri","listeria seeligeri serovar","alicyclobacillus acidoterrestris","Francisella ","staphylococcus epidermidis","acidithiobacillus ferrooxidans")
target_names=c("clostridium_kluyveri_dsm_555","listeria_seeligeri_serovar_1_2b_str_slcc3954","alicyclobacillus_acidoterrestris_atcc_49025","francisella_cf_novicida_fx1","staphylococcus_epidermidis_rp62a","acidithiobacillus_ferrooxidans_atcc_23270")



#fetch type- and species-specific protein sequence
for(i in 1:length(target_species)){

dir.create(target_names[i])

# for misc. idx
above_idx=NULL

temp_cas1_idx=intersect(grep(pattern="cas1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))

cas2_idx=intersect(grep(pattern="cas2",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas2_idx)>0){
 above_idx=c(above_idx,cas2_idx)
 cas2_result=ref_aa[cas2_idx]
 result_path=paste(target_names[i],"cas2.ref.fasta",sep="/")
 print(result_path)
 writeXStringSet(cas2_result,append=T,filepath=result_path)
}

cas3_idx=intersect(grep(pattern="cas3",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas3_idx)>0){
 above_idx=c(above_idx,cas3_idx)
 cas3_result=ref_aa[cas3_idx]
 result_path=paste(target_names[i],"cas3.ref.fasta",sep="/")
 writeXStringSet(cas3_result,append=T,filepath=result_path)
}


cas5_idx=intersect(grep(pattern="cas5",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas5_idx)>0){
 above_idx=c(above_idx,cas5_idx)
 cas5_result=ref_aa[cas5_idx]
 result_path=paste(target_names[i],"cas5.ref.fasta",sep="/")
 writeXStringSet(cas5_result,append=T,filepath=result_path)
}

cas6_idx=intersect(grep(pattern="cas6",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas6_idx)>0){
 above_idx=c(above_idx,cas6_idx)
 cas6_result=ref_aa[cas6_idx]
 result_path=paste(target_names[i],"cas6.ref.fasta",sep="/")
 writeXStringSet(cas6_result,append=T,filepath=result_path)
}

cas7_idx=intersect(grep(pattern="cas7",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas7_idx)>0){
 above_idx=c(above_idx,cas7_idx)
 cas7_result=ref_aa[cas7_idx]
 result_path=paste(target_names[i],"cas7.ref.fasta",sep="/")
 writeXStringSet(cas7_result,append=T,filepath=result_path)
}


cas9_idx=intersect(grep(pattern="cas9",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas9_idx)>0){
above_idx=c(above_idx,cas9_idx)
 cas9_result=ref_aa[cas9_idx]
 result_path=paste(target_names[i],"cas9.ref.fasta",sep="/")
 writeXStringSet(cas9_result,append=T,filepath=result_path)
}


cas10_idx=intersect(grep(pattern="cas10",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cas10_idx)>0){
above_idx=c(above_idx,cas10_idx)
 cas10_result=ref_aa[cas10_idx]
 result_path=paste(target_names[i],"cas10.ref.fasta",sep="/")
 writeXStringSet(cas10_result,append=T,filepath=result_path)
}

cas1_idx=setdiff(temp_cas1_idx,cas10_idx)
if(length(cas1_idx)>0){
above_idx=c(above_idx,cas1_idx)
 cas1_result=ref_aa[cas1_idx]
 result_path=paste(target_names[i],"cas1.ref.fasta",sep="/")
 writeXStringSet(cas1_result,append=T,filepath=result_path)
}

csf1_idx=intersect(grep(pattern="csf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(csf1_idx)>0){
above_idx=c(above_idx,csf1_idx)
 csf1_result=ref_aa[csf1_idx]
 result_path=paste(target_names[i],"csf1.ref.fasta",sep="/")
 writeXStringSet(csf1_result,append=T,filepath=result_path)
}

cpf1_idx=intersect(grep(pattern="cpf1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(cpf1_idx)>0){
above_idx=c(above_idx,cpf1_idx)
 cpf1_result=ref_aa[cpf1_idx]
 result_path=paste(target_names[i],"cpf1.ref.fasta",sep="/")
 writeXStringSet(cpf1_result,append=T,filepath=result_path)
}


c2c1_idx=intersect(grep(pattern="c2c1",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(c2c1_idx)>0){
above_idx=c(above_idx,c2c1_idx)
 c2c1_result=ref_aa[c2c1_idx]
 result_path=paste(target_names[i],"c2c1.ref.fasta",sep="/")
 writeXStringSet(c2c1_result,append=T,filepath=result_path)
}


c2c2_idx=intersect(grep(pattern="c2c2",ignore.case=T,x=ref_id),grep(pattern=target_species[i],ignore.case=T,x=ref_id))
if(length(c2c2_idx)>0){
above_idx=c(above_idx,c2c2_idx)
 c2c2_result=ref_aa[c2c2_idx]
 result_path=paste(target_names[i],"c2c2.ref.fasta",sep="/")
 writeXStringSet(c2c2_result,append=T,filepath=result_path)
}

tmp_idx=grep(pattern=target_species[i],ignore.case=T,x=ref_id)
other_idx=setdiff(tmp_idx,above_idx) 
if(length(other_idx)>0){
 other_result=ref_aa[other_idx]
 result_path=paste(target_names[i],"other.ref.fasta",sep="/")
 writeXStringSet(other_result,append=T,filepath=result_path)
}


}



save.image("all_ref_cas.RData")