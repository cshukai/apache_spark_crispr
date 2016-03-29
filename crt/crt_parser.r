#assuming current directory is the folder containing all the crt output file
out_files=Sys.glob(file.path("*"))

for(i in 1:length(out_files)){
 d=readLines(file(out_files[i]))

}
save.image("crt_parser.RData")