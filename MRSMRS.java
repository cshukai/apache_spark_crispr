import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.io.Serializable;
import org.apache.hadoop.io.Text;
import java.util.*;
import scala.Tuple2;

public class MRSMRS implements Serializable{
	public static void main(String [ ] args) throws Exception{
    	SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
    	JavaSparkContext sc=new JavaSparkContext(conf);   
         // sc.hadoopConfiguration().set("io.serializations","org.apache.hadoop.io.serializer.Serialization");
  
        JavaRDD<String> input=sc.textFile("bacteria/crispr/test/CoarseGrain/word3/part-r-00199");
        MRSMRS mrsmrs=new MRSMRS();
        JavaPairRDD <String, Integer> test=mrsmrs.parseMRSMRStextOutput(input);
        test.saveAsTextFile("crispr_test5");
    	
        System.out.println(test.first());

    	


    }


    public JavaPairRDD <String, Integer> parseMRSMRStextOutput(JavaRDD<String> mrsmrs_output){
        JavaPairRDD <String, Integer> result=mrsmrs_output.flatMapToPair(new PairFlatMapFunction<String,String,Integer>(){
            @Override
            public Iterable<Tuple2<String,Integer>> call(String line){
                ArrayList<Tuple2<String, Integer>> parse_result = new ArrayList<Tuple2<String, Integer>> ();
                String[] temp=line.split("\t");
                String[] temp_2=temp[1].split(";");
                for(int j=0;j<temp_2.length;j++){
                    String[]temp_3=temp_2[j].split(":");
                    parse_result.add(new Tuple2<String,Integer>(temp[0],Integer.parseInt(temp_3[0])));

                }

                return(parse_result);
            }

        });
        
        return(result);

     }
    // output : String :  5'->3': arm  ; Integer[left arm start, right arm start] location is 5'->3' 
    // public  JavaPairRDD<String,int[]> fetchPalindromeArms(JavaPairRDD <String, Integer> seqFilePairRDD,int interval_len){
    // 		seqFilePairRDD.
    // }




}
