piler=readLines(file("null_list_piler"))
piler_null_strains=NULL
for(i in 1:length(piler)){
    piler_null_strains=c(piler_null_strains,unlist(strsplit(x=piler[i],split=":"))[1])
}

crt=readLines(file("null_list_crt"))

crt_null_strains=NULL
for(i in 1:length(crt)){
    crt_null_strains=c(crt_null_strains,unlist(strsplit(x=crt[i],split=":"))[1])
}


crisprdb=readLines(file("null_list_crisprdb"))

crisprdb_null_strains=NULL
for(i in 1:length(crisprdb)){
    if(i>3){
        crisprdb_null_strains=c(crisprdb_null_strains,unlist(strsplit(x=crisprdb[i],split="\\|"))[2])
    }
}

crisprdb_null_strs=gsub(pattern=" ",replacement="_",x=crisprdb_null_strains)


crt_piler_common=intersect(crt_null_strains,piler_null_strains)

crt_db_common=NULL
for(i in 1:length(crt_null_strains)){
   if(length(grep(pattern=crt_null_strains[i],x=crisprdb_null_strs))>0){
        crt_db_common=c(crt_db_common,crt_null_strains[i])
   }
}


piler_db_common=NULL
for(i in 1:length(piler_null_strains)){
   if(length(grep(pattern=piler_null_strains[i],x=crisprdb_null_strs))>0){
        piler_db_common=c(piler_db_common,piler_null_strains[i])
   }
}

