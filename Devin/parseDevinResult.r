load("MRSMRS_preprocessing.RData")
cohort_home=""

#parse out the name of result species
paths=Sys.glob(file.path(cohort_home, "*"))


# contruct identifier for each genome
all_species=NULL
for(i in 1:length(paths)){
temp=unlist(strsplit(paths[i],"/"))
all_species=c(all_species,temp[6])
}




#retreival of corresponding genome fasta file
unzipped_genome_paths=Sys.glob(file.path(bac_collectoin_home, "*", "*","dna","*.dna.chromosome.Chromosome.fa"))


#validation -- assuming there is only one single position for the corresponding kmer
library(ShortRead)
library(Biostrings)
library(rhdfs)
for(i in 1:length(all_species)){
    # result
    allPartFiles=Sys.glob(file.path(paths[i],"part-*[0-9]"))
    thisResult=NULL
    for(j in 1:length(allPartFiles)){
      thisPartFile=readLines(file(allPartFiles[j]))
      thisPartFile=gsub(pattern="CompactBuffer",replacement="",thisPartFile)
      thisPartFile=gsub(pattern="\\(",replacement="",thisPartFile)
      thisPartFile=gsub(pattern="\\)",replacement="",thisPartFile)
      for(k in 1:length(thisPartFile)){
          thisLine=unlist(strsplit(split=",",x=thisPartFile[k]))
          thisResult=rbind(thisResult,c(thisLine[1],thisLine[3]))
      
      }

    } 
    # ref 
    this_species=all_species[i]
    this_fasta=unzipped_genome_paths[grep(pattern=this_species,x=unzipped_genome_paths)]
    d=readFasta(this_fasta)
    sq=toString(unlist(sread(d)))
    
    for(a in 1:nrow(thisResult)){
        k_len=30
        thisSeq=thisResult[a,1]
        this_position=as.numeric(thisResult[a,2])
        this_end=this_position+k_len-1
        this_ref=substr(sq,start=this_position,stop=this_end)
        if(this_position>0 ){
        
           if(thisSeq!=this_ref){
              print(a)
           }
        }
        
        if(this_position<0){
        
        }
        
    }
    
    
}

save.image("MRSMRSresult.procee.RData")
