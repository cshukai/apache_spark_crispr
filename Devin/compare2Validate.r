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
#input
load("/home/shchang/scratch/crispr_arr/validate.RData")
mrsmrs=read.table(mrsmrs_summary,sep="\t",fill=T)
crt=read.csv("crt_summary.csv",header=T)
piler=read.csv(PILER_summary_file,header=T)
mrsmrs2=mrsmrs[which(mrsmrs[,1] %in% species),]

# species analysis
mrsmrs_species=names(table(as.character(mrsmrs2[,1])))
crt_species=names(table(crt[,1]))
piler_species=names(table(piler[,1]))


length(species)
length(unique(mrsmrs_species))
length(unique(crt_species))
length(unique(piler_species))


m_c_common=intersect(mrsmrs_species,crt_species)
m_p_common=intersect(mrsmrs_species,piler_species)
c_p_common=intersect(crt_species,piler_species)


mrsmrs_specific=setdiff(setdiff(mrsmrs_species,crt_species),piler_species)
# filtering out over-extended 
mrsmrs3=mrsmrs[which(mrsmrs[,1] %in% mrsmrs_specific),]
write.csv(mrsmrs3,file="../mrsmrs.specifi.csv",row.names=F)
#lewis3
mrsmrs3=read.csv("mrsmrs.specifi.csv",header=T)
fasta_home="/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta"
fasta=Sys.glob(file.path(fasta_home, "*","*","dna","*dna.chromosome.Chromosome.fa"))
save.image("validation.RData")
library(ShortRead)
library(Biostrings)
reserverdList=NULL
for(i in 2726:length(mrsmrs_species)){
    this_file=fasta[grep(pattern=mrsmrs_species[i],fasta,ignore.case=T)]
    print(i)
    print("+++++++")
    if(length(this_file)==1){
        d=readFasta(this_file[1])
        x=toString(unlist(sread(d)))
        this_d=mrsmrs[which(mrsmrs[,1] %in% mrsmrs_species[i]),]
        all_array_id=unique(this_d[,2])
        for(j in 1:length(all_array_id)){
            this_array_d=this_d[which(this_d[,2] %in% all_array_id[j] ),]
            all_seq=NULL
            for(k in 1:nrow(this_array_d)){
                this_seq_start=this_array_d[k,3]
                this_seq_end=this_array_d[k,3]+this_array_d[k,4]-1
                this_seq=substr(x,this_seq_start,this_seq_end)
                all_seq=c(all_seq,this_seq)
            }
            
            if(sum(diff(nchar(all_seq)))==0 &&length(which(is.na(all_seq)))==0){
                 for(k in 1:nchar(all_seq[1])){
                     estimator=nucleotideFrequencyAt(DNAStringSet(all_seq),at=1,as.prob=T)
                     if(max(estimator)<0.75){
                         break
                     }
                 }
                 if(k==nchar(all_seq[1])){
                     reserverdList=rbind(reserverdList,this_array_d)
                 }
            }
       
        }
    
    }
    else{
        print(this_file)
        print(i)
        print("-----")
    
    }
}

refineList=NULL
for(i in 1:nrow(reserverdList)){
            print(i)
            this_d=reserverdList[which(reserverdList[,1] %in% mrsmrs_species[i]),]
            all_array_id=unique(this_d[,2])
            for(j in 1:length(all_array_id)){
            this_array_d=this_d[which(this_d[,2] %in% all_array_id[j] ),]
            this_start=as.numeric(this_array_d[1,3])
            this_end=this_array_d[nrow(this_array_d),3]+this_array_d[nrow(this_array_d),4]-1
            this_arr_size=this_end-this_start+1
            if(this_arr_size>=75 && length(this_arr_size)>0){
                refineList=rbind(refineList,this_array_d)
            }

            }
        }

#arch analysis
crt_rep_seq=as.character(crt[,4])
piler_rep_seq=as.numeric(piler[,4])
summary(nchar(crt_rep_seq))
summary(piler_rep_seq)

#novel structure
d=readFasta("/home/shchang/data/bac_29_fasta/ftp.ensemblgenomes.org/pub/release-29/bacteria/fasta/bacteria_20_collection/acetobacter_pasteurianus_ifo_3283_03/dna/Acetobacter_pasteurianus_ifo_3283_03.GCA_000010845.1.29.dna.chromosome.Chromosome.fa")
x=toString(unlist(sread(d)))
substr(x,2212367,2212421)
substr(x,2212376,2212376+55-1)
substr(x,2212385,2212385+55-1)

#venn diagram
library(gplots)
venn( list(Spark=mrsmrs_species,CRT=crt_species,PilerCR=piler_species) )



m_c_array_num=NULL


for(i in 1:length(m_c_common)){
    this_species=m_c_common[i]
    these_m_array_num=m
}
