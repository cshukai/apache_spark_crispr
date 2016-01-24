#1.build ref db                                                                             
/share/sw/blast/2.2.30+/bin/makeblastdb -in ../tmp/francisella_cf_novicida_fx1   -dbtype prot -parse_seqids  -logfile log.txt -out blastdb/ref.db                                        

#2. blastp                                                                                  
/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas1.ref.fasta -out cas1.blastp.out.txt  -outfmt 6                                                                     

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas2.ref.fasta -out cas2.blastp.out.txt  -outfmt 6                                                                     


/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query other.ref.fasta -out other.blastp.out.txt  -outfmt 6

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas3.ref.fasta -out cas3.bla\
stp.out.txt  -outfmt 6

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas5.ref.fasta -out cas5.bla\
stp.out.txt  -outfmt 6

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas6.ref.fasta -out cas6.bla\
stp.out.txt  -outfmt 6

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas7.ref.fasta -out cas7.bla\
stp.out.txt  -outfmt 6

/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas9.ref.fasta -out cas9.bla\
stp.out.txt  -outfmt 6


/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cas10.ref.fasta -out cas10.b\
lastp.out.txt  -outfmt 6


/share/sw/blast/2.2.30+/bin/blastp  -db blastdb/ref.db  -query cpf1.ref.fasta -out cpf1.bla\
stp.out.txt  -outfmt 6