home_dir="/home/shchang/scratch/crispr_arr/mrsmrs/classI_result"
paths=Sys.glob(file.path(home_dir, "*"))
species=NULL

# parsing the file structure
for(i in 1:length(paths)){
    tmp=unlist(strsplit(x=paths[i],split="/"))
    this_species_tmp=tmp[length(tmp)]
    tmp2=unlist(strsplit(x=this_species_tmp,split="\\."))
    this_species=tmp2[1]
    print(this_species)
    species=c(species,this_species)
}
save.image("../validate.RData")


# parsing the crispr_mrsmrs result to tabular format.
result=NULL
for(i in 1:length(paths)){
    partFiles=Sys.glob(file.path(paths[i], "crispr_test2","part-*"))
    for(j in 1:length(partFiles)){
        d=readLines(file(partFiles[j]))
        if(length(d)>0){
            for(k in 1:length(d)){
                item=d[k]
                d_clean=gsub(gsub(x=gsub(x=item,pattern="\\(",replacement=""),pattern="\\]\\)",replacement=""),pattern="\\[",replacement="")
                tmp=unlist(strsplit(x=d_clean, split=","))
                unit_positions=as.numeric(tmp[2:length(tmp)])
                
                for(m in 1:length(unit_positions)){
                    if(m %% 2==1){
                        this_arr_id=k
                        this_unit_start=unit_positions[m]
                        this_unit_end=unit_positions[m+1]
                        this_species=species[i]# the order of species and paths are the same
                        unit_len=this_unit_end-this_unit_start+1;
                        this_row=c(this_species,this_arr_id,this_unit_star,unit_len)
                        result=rbind(result,this_row)
                    }
                }
            }
        }
    }
    
    
}


colnames(result)=c("species","array_id","unit_start","unit_len")
save.image("../validate.RData")


# comparison
CRT_summary_file="/home/shchang/scratch/crispr_arr/crt"
PILER_summary_file="/home/shchang/scratch/crispr_arr/pilercr"