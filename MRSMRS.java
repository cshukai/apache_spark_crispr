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
        JavaRDD<String> input_2=sc.textFile("bacteria/crispr/test2/CoarseGrain/word10/");
        MRSMRS mrsmrs=new MRSMRS();


        // JavaPairRDD<String,Integer> input=sc.sequenceFile("bacteria/crispr/test/limeload",String.class,Integer.class);
        // input.saveAsTextFile("crispr_test");

        JavaPairRDD <String, Integer> test=mrsmrs.parseMRSMRStextOutput(input);

        // JavaPairRDD <Integer,Integer> test_2=mrsmrs.fetchPalindromeArms( test,0,6,6); // 80782 error is here
        JavaPairRDD <Integer,Integer> test_2=mrsmrs.fetchPalindromeArms( test,0,6,3);
        // JavaPairRDD <Integer,Integer> test_10=mrsmrs.parsedMRSMRSresult()
        // test.saveAsTextFile("crispr_test");
        test_2.saveAsTextFile("crispr_test5");
    	JavaRDD<String> fasta=sc.textFile("bacteria/crispr/data/Methanocaldococcus_jannaschii_dsm_2661.GCA_000091665.1.26.dna.chromosome.Chromosome.fa");
        JavaPairRDD <Integer,Integer> test_3= mrsmrs.filterOutBadMrsMrsResult(fasta,test_2,6);
        // test_3.saveAsTextFile("crispr_test_2");


        JavaPairRDD <String, Integer> test_4=mrsmrs.parseMRSMRStextOutput(input_2);
        JavaPairRDD <String, Integer> test_5=mrsmrs.flagMrsMrsRepeatWithArmInside(test_4,10, test_2);
        test_5.saveAsTextFile("crispr_test_2");


        JavaPairRDD<String,ArrayList<Integer>> test6=mrsmrs.suggestCrisprBorder(test_5);
        test6.saveAsTextFile("crispr_test");

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
   
           JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=parsedMRSMRSresult.groupByKey();

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
                                          
                                            possibleRepeatUnits.add(new Tuple2<Integer,Integer>(thisNegLoc_corrected,thisPosLoc_corrected));
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

    // filter out incorrect repeat happenning in limeload step in MRSMRS
    public JavaPairRDD <Integer, Integer> filterOutBadMrsMrsResult(JavaRDD<String> fasta_seqs,JavaPairRDD <Integer, Integer> unfiltered_result,int armLen){
           final List<String> fastaSeqs=fasta_seqs.collect();
           final int totalArmLen=armLen;
           JavaPairRDD <Integer, Integer> result_filtered=unfiltered_result.filter(new Function<Tuple2<Integer,Integer>, Boolean>(){
            @Override
            public Boolean call(Tuple2<Integer,Integer> keyValue){
                boolean isAccurate=false;
                int thisPosLoc_start=keyValue._2();
                int thisNegLoc_start=keyValue._1();
                int thisPosLoc_end=thisPosLoc_start+totalArmLen-1;
                int thisNegLoc_end=thisNegLoc_start+totalArmLen-1;

                String thisLeftArmSeq=getSubstring(fastaSeqs,thisPosLoc_start,thisPosLoc_end);
                String thisRightArmSeq=getSubstring(fastaSeqs,thisNegLoc_start,thisNegLoc_end);

                
                int left_balance=0;
                int right_balance=0;
                for(int j=0;j<thisLeftArmSeq.length();j++){
                    Character left_chr=thisLeftArmSeq.charAt(j);
                    Character right_chr=thisRightArmSeq.charAt(j);
                    if(left_chr=='A'){
                       left_balance=left_balance+1;
                    }

                    if(left_chr=='T'){
                       left_balance=left_balance-1;
                    }

                    if(left_balance=='C'){
                        left_balance=left_balance+2;
                    }

                    if(left_balance=='G'){
                        left_balance=left_balance-2;
                    }


                    if(right_chr=='A'){
                       right_balance=right_balance+1;
                    }

                    if(right_chr=='T'){
                       right_balance=right_balance-1;
                    }

                    if(right_balance=='C'){
                        right_balance=right_balance+2;
                    }

                    if(right_balance=='G'){
                        right_balance=right_balance-2;
                    }


                }

                int tot_balance=left_balance+right_balance;
                if(tot_balance==0){
                    isAccurate=true;
                }
               
                return(isAccurate);
            }

        });

        return(result_filtered);

    }


    public String getSubstring(List<String> seqFile, int start_loc, int end_loc){

        int startLine=(int)Math.ceil(start_loc/60)+1;
        int endLine=(int)Math.ceil(end_loc/60)+1;
        int startLocIdxInLine=0;
        int endLocIdxInLine=0;
        if(start_loc%60==0){
            startLine=startLine-1;
            startLocIdxInLine=59;
        }

        else{
            startLocIdxInLine=start_loc-(startLine-1)*60-1;
        }

        if(end_loc%60==0){
            endLine=endLine-1;
            endLocIdxInLine=59;
        }
        else{
            endLocIdxInLine=end_loc-(endLine-1)*60-1;    
        }
        
        
        

        
        String result="";
        String part=seqFile.get(startLine);
        
        if(startLine==endLine){
            if(end_loc%60==0){

                result=part.substring(startLocIdxInLine,endLocIdxInLine+1); 
            }
            else{

               result=part.substring(startLocIdxInLine,endLocIdxInLine+1);   
           }  

       }
       else{
        String middlePart="";
        if(endLine-startLine>1){


         for(int n=1;n<endLine-startLine;n++){
            middlePart=middlePart+seqFile.get(startLine+n);
        }


    }


    
    if(start_loc%60==0){
     result=part.charAt(startLocIdxInLine)+middlePart+seqFile.get(endLine).substring(0,endLocIdxInLine+1);
 }

 else{
     if(end_loc%60==0){
        result=part.substring(startLocIdxInLine,part.length())+middlePart+seqFile.get(endLine).charAt(endLocIdxInLine); 
    }
    else{


        result=part.substring(startLocIdxInLine,part.length())+seqFile.get(endLine).substring(0,endLocIdxInLine+1);

    }
}

}


return(result);

}
    //output :  left: start location of a specific repat unit with a particular length  right: arm start locations in a specific repat unit with a particular length
    public JavaPairRDD <String,Integer> flagMrsMrsRepeatWithArmInside (JavaPairRDD <String, Integer> mrsmrs_repeats,int mrsmrs_repeat_len,JavaPairRDD <Integer,Integer> arm_start_locs){
    

        // filter out mrsmrs repeat that doesn't have arm inside
        final List<Integer> left_arm_starts=arm_start_locs.keys().collect();
        final List<Integer> right_arm_starts=arm_start_locs.values().collect();
        final int repeat_len=mrsmrs_repeat_len;
        JavaPairRDD<String,Integer> mrsmrs_repeats_filtered=mrsmrs_repeats.filter(new Function<Tuple2<String, Integer>, Boolean>(){
            @Override
            public Boolean call(Tuple2<String, Integer> keyValue){
                int repeat_unit_start=keyValue._2();
                int repeat_unit_end=repeat_unit_start+repeat_len;
                boolean containArm=false;
                for(int j=repeat_unit_start;j<=repeat_unit_end;j++){
                    if(left_arm_starts.contains(j)){
                        containArm=true;
                    }

                    if(right_arm_starts.contains(j)){
                        containArm=true;
                    }

                }


                return(containArm);
            }

        });
        return(mrsmrs_repeats_filtered);
        
        


    }


 
    public JavaPairRDD<String,ArrayList<Integer>>  suggestCrisprBorder(JavaPairRDD <String, Integer> completeListOfFlaggedRepeats ){
        JavaPairRDD <String,Iterable<Integer>>flaggedRepeats_sorted=completeListOfFlaggedRepeats.groupByKey();   


        JavaPairRDD<String,ArrayList<Integer>> cirsprs=flaggedRepeats_sorted.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Integer>>,String,ArrayList<Integer>>(){
            @Override
            public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,Iterable<Integer>> keyValue){
                Iterable<Integer> mrsmrs_repeat_locs=keyValue._2();
                Iterator<Integer> itr=mrsmrs_repeat_locs.iterator();
                ArrayList<Integer> repeat_locs=new ArrayList<Integer>();
                 while(itr.hasNext()){
                     repeat_locs.add(itr.next());
                }

                Collections.sort(repeat_locs);
                ArrayList<Tuple2<String, ArrayList<Integer>>> result = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                result.add(new Tuple2<String, ArrayList<Integer>>(keyValue._1(),repeat_locs));

                return(result);

            }


        });


        return(cirsprs);
    }


}
