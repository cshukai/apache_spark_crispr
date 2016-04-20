rm -rf mrsmrs
rm -rf crispr*
rm -rf palin*
rm *.log.txt
/home/shukai/Downloads/spark-1.6.0-bin-hadoop2.6/bin/spark-submit  --class "MR
SMRS" ./target/simple-project-1.0.jar