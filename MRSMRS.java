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
        test.saveAsTextFile("crispr_test");
        test_2.saveAsTextFile("crispr_test5");
    	
        System.out.println(test_2.first());

    	// JavaPairRDD<String,Integer> test3=sc.sequenceFile("protist/CoarseGrain/word20",String.class,Integer.class);
     //    test3.saveAsTextFile("crispr_test_2");


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
    public  JavaPairRDD <Integer, Integer> fetchPalindromeArms(JavaPairRDD <String, Integer> parsedMRSMRSresult,int min_interval_len,int max_interval_len,int armlen){
           final  int min_dist=2*armlen+min_interval_len;
           final  int max_dist=2*armlen+max_interval_len;
           final  int arm_len=armlen;
            JavaPairRDD <String, Integer> parsedMRSMRSresult_filtered=parsedMRSMRSresult.filter(new Function<Tuple2<String, Integer>, Boolean>(){
                @Override
                public Boolean call(Tuple2<String,Integer> keyValue){
                    
                    return(keyValue._1().length()==arm_len);
                }

            });
    	 	JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=parsedMRSMRSresult_filtered.groupByKey();
            JavaPairRDD<Integer, Integer> result= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,Integer, Integer>(){
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
                 int iterationNum=locs_on_postiveStrand.size();
                 for(int j=0;j<iterationNum;j++){
                     int thisPosLoc=locs_on_postiveStrand.get(j);
                     if(j<iterationNum-2){
                        int nextPosLoc=locs_on_postiveStrand.get(j+1);
                        int nextTwoPosLoc=locs_on_postiveStrand.get(j+2); 
                        int firstDist_pos=nextPosLoc-thisPosLoc;
                        int secondDist_pos=nextTwoPosLoc-nextPosLoc;
                        if(firstDist_pos<140 && secondDist_pos<140){
                            int iterationNum_neg=locs_on_negStrand.size();
                            for(int k=0;k<iterationNum_neg;k++){
                                int thisNegLoc=locs_on_negStrand.get(k);
                                if(k<iterationNum_neg-2){
                                    int firstDist_neg=locs_on_negStrand.get(k+1)-thisNegLoc;
                                    int secondDist_neg=locs_on_negStrand.get(k+2)-locs_on_negStrand.get(k+1);
                                    if(firstDist_neg<140 && secondDist_neg<140){
                                        int size=thisNegLoc-thisPosLoc;
                                        if(size>=min_dist && size<=max_dist){
                                            int intervalSize=thisNegLoc-thisPosLoc-arm_len*2;
                                            int thisPosLoc_corrected=thisPosLoc+1;
                                            int thisNegLoc_corrected=thisNegLoc-2;
                                            // String negSide=thisNegLoc_corrected+":"+keyValue._1();
                                            // String posSide=thisPosLoc_corrected+":"+keyValue._1();
                                            possibleRepeatUnits.add(new Tuple2<Integer,Integer>(thisNegLoc_corrected,thisPosLoc_corrected));
                                            // possibleRepeatUnits.add(new Tuple2<String,String>(negSide,posSide));
                                        }   
                                    }
                                          
                                }
                           
                            }
                            
                        }
                     }
               
                 }

                 return(possibleRepeatUnits);

                }

            });

        return(result);

            

    }




}
