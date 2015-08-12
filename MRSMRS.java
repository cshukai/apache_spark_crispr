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
import java.util.*;
import scala.Tuple2;

public class MRSMRS implements Serializable{
	public static void main(String [ ] args) throws Exception{
    	SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
    	JavaSparkContext sc=new JavaSparkContext(conf);   
  
        JavaRDD<String> input=sc.textFile("bacteria/crispr/test/CoarseGrain/word3/");
        MRSMRS mrsmrs=new MRSMRS();
        JavaPairRDD <String, Integer> test=mrsmrs.parseMRSMRStextOutput(input);
        JavaPairRDD <Integer,Integer> test_2=mrsmrs.fetchPalindromeArms( test,0,6,6);

        test_2.saveAsTextFile("crispr_test5");
    	
        System.out.println(test_2.first());

    	


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


    // output : Integer : interval length  ;  left arm start -location is 5'->3' 
    public  JavaPairRDD <Integer,Integer> fetchPalindromeArms(JavaPairRDD <String, Integer> parsedMRSMRSresult,int min_interval_len,int max_interval_len,int armlen){
           final  int min_dist=2*armlen+min_interval_len;
           final  int max_dist=2*armlen+max_interval_len;
           final  int arm_len=armlen;
    	 	JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=parsedMRSMRSresult.groupByKey();
            JavaPairRDD<Integer,Integer> result= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,Integer,Integer>(){
                @Override
                public Iterable<Tuple2<Integer,Integer>> call(Tuple2<String, Iterable<Integer>> keyValue){
                 Iterable<Integer>data =keyValue._2();
                 Iterator<Integer> itr=data.iterator();
                 ArrayList<Integer> locs_on_postiveStrand=new ArrayList<Integer>();
                 ArrayList<Integer> locs_on_negStrand=new ArrayList<Integer>();
                 ArrayList<Tuple2<Integer, Integer>> possibleRepeatUnits = new ArrayList<Tuple2<Integer, Integer>> ();

                 while(itr.hasNext()){
                   int thisLoc=itr.next();
                   if(thisLoc>0){
                      locs_on_postiveStrand.add(thisLoc);
                   }
                   else{
                      locs_on_negStrand.add(Math.abs(thisLoc));
                   }

                 }

                 Collections.sort(locs_on_postiveStrand);
                 Collections.sort(locs_on_negStrand);
                 for(int j=0;j<locs_on_postiveStrand.size();j++){
                     int thisPosLoc=locs_on_postiveStrand.get(j);
                     for(int k=0;k<locs_on_negStrand.size();k++){
                         int thisNegLoc=locs_on_negStrand.get(k);
                         int size=thisNegLoc-thisPosLoc;
                         if(size>=min_dist|| size<max_dist){
                            int intervalSize=thisNegLoc-thisPosLoc+arm_len*2;
                            possibleRepeatUnits.add(new Tuple2<Integer,Integer>(intervalSize,thisPosLoc));
                         }
                     }
                 }

                 return(possibleRepeatUnits);

                }

            });

        return(result);

    }




}
