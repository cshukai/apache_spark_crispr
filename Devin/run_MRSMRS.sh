# cluster d
rm -rf mrsmrs
rm -rf crispr*
rm -rf palin*
rm *.log.txt
/home/shukai/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit  --class "MR
SMRS" ./target/simple-project-1.0.jar


# mri
srun -N 1 -p Compute hadoop fs -rm -r /idas/sc724/mrsmrs
srun -N 1 -p Compute hadoop fs -rm -r /idas/sc724/palindrome
srun -N 1 -p Compute hadoop fs -rm -r /idas/sc724/crispr_test2
srun -N 1 -p Compute hadoop fs -rm -r /idas/sc724/crispr_test3
spark-submit  --class "MRSMRS"  --num-executors 4  ./target/simple-project-1.0.jar > spark.log.txt
