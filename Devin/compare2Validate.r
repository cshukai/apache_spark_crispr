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
    if(length(partFiles)>0){
        array_id=0;
    for(j in 1:length(partFiles)){
        con=file(partFiles[j])
        d=readLines(con)
        if(length(d)>0){
        d=unique(d)
        array_id=array_id+1;
            for(k in 1:length(d)){
                item=d[k]
                d_clean=gsub(gsub(x=gsub(x=item,pattern="\\(",replacement=""),pattern="\\]\\)",replacement=""),pattern="\\[",replacement="")
                tmp=unlist(strsplit(x=d_clean, split=","))
                unit_positions=as.numeric(tmp[2:length(tmp)])
                
                for(m in 1:length(unit_positions)){
                    if(m %% 2==1){
                        this_arr_id=array_id
                        this_unit_start=unit_positions[m]
                        this_unit_end=unit_positions[m+1]
                        this_species=species[i]# the order of species and paths are the same
                        unit_len=this_unit_end-this_unit_start+1;
                        this_row=c(this_species,this_arr_id,this_unit_start,unit_len)
                        result=rbind(result,this_row)
                        #print(nrow(result))
                      print(partFiles[j])
                
                     # cat(this_row,file="/home/shchang/scratch/crispr_arr/mrsmrs/classI.summary.txt",append=T,fill=T)
                    }
                }
            }
        }
        close(con)
    }
        
    }
}


colnames(result)=c("species","array_id","unit_start","unit_len")
save.image("../validate.RData")


############################comparison##################################
mrsmrs_summary="classI.summary.txt"
CRT_summary_file="/home/shchang/scratch/crispr_arr/crt*.csv"
PILER_summary_file="/home/shchang/scratch/crispr_arr/pilercr"

mrsmrs=read.table(mrsmrs_summary,sep="\t",fill=T)
mrsmrs_species=names(table(mrsmrs[,1]))

crt=read.csv("crt_summary.csv",header=T)
crt_species=names(table(crt[,1]))

m_c_array_num=NULL
m_c_common=intersect(mrsmrs_species,crt_species)
for(i in 1:length(m_c_common)){
    this_species=m_c_common[i]
    these_m_array_num=m
}


length(mrsmrs_species)
length(crt_species)
