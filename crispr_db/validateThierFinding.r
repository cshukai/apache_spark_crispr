without=read.table("noCrisprSpecies.txt",quote = "",header=F,sep="|")
temp=tolower(without[,2])
temp2=gsub(pattern=" ",replacement="_",temp)
temp3=gsub(pattern="\'",replacement="",temp2)
temp4=gsub(pattern="\\/",replacement="_",temp3)
temp5=gsub(pattern="-",replacement="_",temp4)
temp6=gsub(pattern="\\.",replacement="",temp5)
temp7=gsub(pattern="\\(",replacement="",temp6)
temp8=gsub(pattern="\\)",replacement="",temp7)

without[,2]=temp8
write.csv(without,"without.csv",row.names=F)
save.image("parse.RData")
