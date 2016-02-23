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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.*;
import scala.Tuple2;
import org.apache.hadoop.io.Text;

public class MRSMRS implements Serializable{
	public static void main(String [ ] args) throws Exception{
        //env configuration
        SparkConf conf=new SparkConf().setAppName("spark-crispr");
    	JavaSparkContext sc=new JavaSparkContext(conf);   
        MRSMRS mrsmrs=new MRSMRS();
        //input
       // String path1=args[0];
        //String path2=args[1];
        JavaRDD<String> input=sc.textFile("30mer/Clostridium_kluyveri_dsm_555.GCA_000016505.1.29.dna.chromosome.Chromosome.fa");
        JavaRDD<String> input_2=sc.textFile("30mer/Streptococcus_thermophilus_cnrz1066.GCA_000011845.1.29.dna.chromosome.Chromosome.fa");//for regions nearby tracr's repeat
        //process
         JavaPairRDD<String,Integer> test=mrsmrs.parseDevinOutput(input);
         //test.saveAsTextFile("crispr_test");
         JavaPairRDD <String,ArrayList<Integer>> test_2=mrsmrs.extractRepeatPairCandidate(test,50,20,30);
         //test_2.saveAsTextFile("crispr_test5");
         //JavaPairRDD<String,ArrayList<Integer>> test_3=mrsmrs.debug(test,test_2,0.5, 4,3,8);
         // test_3.saveAsTextFile("crispr_test");
         ArrayList<Tuple2<String,ArrayList<Integer>>> test_3=mrsmrs.extractInsideStemLoopRepeatPairs(test,test_2,0.5, 4,3,8);
            System.out.print("=============================::::::::::::::::::::::::::::::;"+test_3.size());
            for(int i=0; i < test_3.size();i++){
                System.out.println(test_3.get(i));
                
            }

    }

    //output: {seq,[repeatPart1_start,repeatPairPart2_start,tracr_palindromeArm1_start,in_arm1_start,i]}
    public ArrayList<Tuple2<String,ArrayList<Integer>>> extractInsideStemLoopRepeatPairs(JavaPairRDD<String,Integer>parsedMRSMRSresult,JavaPairRDD<String,ArrayList<Integer>> repeatPairs,double tracer_repeat_similarity,int min_arm_len,int min_loop_size,int max_loop_size){
        final int minLoopSize= min_loop_size;
        final int maxLoopSize= max_loop_size;
        final int arm_len= min_arm_len; 
        
        
        //output : {unit seqeunce:kmer sequence,[unit_start,singleKmer_start]}
        JavaPairRDD<String,ArrayList<Integer>> repeat_pair_kmers=repeatPairs.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<Integer>>(){
                @Override
                public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,ArrayList<Integer>> keyValue){
                    String thisSeq=keyValue._1();
                    ArrayList<Integer> seq_starts=keyValue._2();
                    int first_portion_star=seq_starts.get(0);
                    
                    ArrayList<Tuple2<String, ArrayList<Integer>>>result = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                    //to record sequce and start position of every record
                    // k in k mers equal to min arm length
                     List<Tuple2<String,ArrayList<Integer>>> kmer_list =new ArrayList<Tuple2<String,ArrayList<Integer>>>();
                    for(int i=0;i<thisSeq.length()-arm_len+1;i++){
                        String kmer=thisSeq.substring(i,i+arm_len);
                        ArrayList<Integer> locs=new ArrayList<Integer>();
                        locs.add(first_portion_star);
                        locs.add(i+first_portion_star);
                        String string_key=thisSeq+":"+kmer;
                        kmer_list.add(new Tuple2<String,ArrayList<Integer>>(string_key,locs));
                    }
                    

                  
                        return(kmer_list);
                    }
                    
               });
               
               
      JavaPairRDD<String, ArrayList <Integer>> repeatKmersGrouped = repeat_pair_kmers.reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
         public ArrayList<Integer> call(ArrayList<Integer> kmer_pos1,ArrayList<Integer>kmer_pos2) {
              ArrayList<Integer> aggregatedList= new ArrayList<Integer>();
              aggregatedList.add(kmer_pos1.get(0));
              aggregatedList.add(kmer_pos1.get(1));
              aggregatedList.add(kmer_pos2.get(0));
              aggregatedList.add(kmer_pos2.get(1));
              return(aggregatedList);
        }
     });
      
      
    final List<String> unit_kmer=repeatKmersGrouped.keys().collect();
    ArrayList<Tuple2<String, ArrayList<Integer>>> result=new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
      
     ArrayList<String> kmer_seq=new ArrayList<String>();
     ArrayList<String> unit_seq=new ArrayList<String>();
     
      for(int i=0;i<unit_kmer.size();i++){
          String []temp=unit_kmer.get(i).split(":");
          kmer_seq.add(temp[1]);
          unit_seq.add(temp[0]);
      
      }
      
      
     for(int i=0;i<kmer_seq.size();i++){
         String this_kmer_seq=kmer_seq.get(i);
          String corr_seq="";
                        for(int j=0;j<this_kmer_seq.length();j++){
                            Character thisCharacter=this_kmer_seq.charAt(j);
                            String thisChar=thisCharacter.toString();
                            if(thisChar.equals("A")){
                                corr_seq=corr_seq+"T";
                            }
                            if(thisChar.equals("T")){
                                corr_seq=corr_seq+"A";
                            }
                            if(thisChar.equals("C")){
                                corr_seq=corr_seq+"G";
                            }
                             if(thisChar.equals("G")){
                                corr_seq=corr_seq+"C";
                            }
                        }

                   
                  if(i<kmer_seq.size()-2){
                                for(int j=i+2;j<kmer_seq.size();j++){
                                    if(kmer_seq.get(j).equals(corr_seq)){
                                        String thisKey=unit_seq.get(i)+":"+kmer_seq.get(i);
                                        String thatKey=unit_seq.get(j)+":"+kmer_seq.get(j);
                                        List<ArrayList<Integer>>theseLocations=repeatKmersGrouped.lookup(thisKey);
                                        List<ArrayList<Integer>>corrLocations=repeatKmersGrouped.lookup(thatKey);
                                        for(int k=0;k<theseLocations.size();k++){
                                            for(int m=0;m<corrLocations.size();m++){
                                                  for(int a=0;a<theseLocations.get(k).size();a++){
                                                      for(int b=0;b<theseLocations.get(m).size();b++){
                                                          if(a%2==1 && b%2==1){
                                                              int loopsize=corrLocations.get(m).get(b)-theseLocations.get(k).get(a)-1 ;
                                                          if(loopsize>=minLoopSize && loopsize<=maxLoopSize){
                                                           ArrayList<Integer>positions=new ArrayList<Integer>();
                                                           positions.add(theseLocations.get(k).get(a));
                                                           positions.add(corrLocations.get(m).get(b));
                                                        
                                                           result.add(new Tuple2<String, ArrayList<Integer>>(unit_seq.get(i),positions));
                                                }
                                                          }
                                                      }
                                                  }
                                                  
                                                
                                            }
                                         }
                        
                                    }
                                }
                  }
     }
      return(result);
    }    
             
        //generate k_mer for internal  and external repeat unit
        
       //put out potential regions for analysis of exisitence of stem-loop
        // filter out repeat pairs not associated with stem -loop
        // if no repeat pair left after such filtration, downsize k and binary search for larget possible k
        
        
       
     public JavaPairRDD<String,ArrayList<Integer>> debug(JavaPairRDD<String,Integer>parsedMRSMRSresult,JavaPairRDD<String,ArrayList<Integer>> repeatPairs,double tracer_repeat_similarity,int min_arm_len,int min_loop_size,int max_loop_size){
        final int minLoopSize= min_loop_size;
        final int maxLoopSize= max_loop_size;
        final int arm_len= min_arm_len; 
        
        
        //output : {unit seqeunce:kmer sequence,[unit_start,singleKmer_start]}
        JavaPairRDD<String,ArrayList<Integer>> repeat_pair_kmers=repeatPairs.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<Integer>>(){
                @Override
                public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,ArrayList<Integer>> keyValue){
                    String thisSeq=keyValue._1();
                    ArrayList<Integer> seq_starts=keyValue._2();
                    int first_portion_star=seq_starts.get(0);
                    
                    ArrayList<Tuple2<String, ArrayList<Integer>>>result = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                    //to record sequce and start position of every record
                    // k in k mers equal to min arm length
                    
                     List<Tuple2<String,ArrayList<Integer>>> kmer_list =new ArrayList<Tuple2<String,ArrayList<Integer>>>();
                    for(int i=0;i<thisSeq.length()-arm_len+1;i++){
                        String kmer=thisSeq.substring(i,i+arm_len);
                        ArrayList<Integer> locs=new ArrayList<Integer>();
                        locs.add(first_portion_star);
                        locs.add(i+first_portion_star);
                        String string_key=thisSeq+":"+kmer;
                        kmer_list.add(new Tuple2<String,ArrayList<Integer>>(string_key,locs));
                    }
                    

                  
                        return(kmer_list);
                    }
                    
               });
        
        
        JavaPairRDD<String, ArrayList <Integer>> repeatKmersGrouped = repeat_pair_kmers.reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
         public ArrayList<Integer> call(ArrayList<Integer> kmer_pos1,ArrayList<Integer>kmer_pos2) {
              ArrayList<Integer> aggregatedList= new ArrayList<Integer>();
              aggregatedList.add(kmer_pos1.get(0));
              aggregatedList.add(kmer_pos1.get(1));
              aggregatedList.add(kmer_pos2.get(0));
              aggregatedList.add(kmer_pos2.get(1));
              return(aggregatedList);
        }
       });
              
        
    final List<String> unit_kmer=repeatKmersGrouped.keys().collect();
    ArrayList<Tuple2<String, ArrayList<Integer>>> result=new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
      
     ArrayList<String> kmer_seq=new ArrayList<String>();
     ArrayList<String> unit_seq=new ArrayList<String>();
     
      for(int i=0;i<unit_kmer.size();i++){
          String []temp=unit_kmer.get(i).split(":");
          kmer_seq.add(temp[1]);
          unit_seq.add(temp[0]);
      
      }
      
      
     for(int i=0;i<kmer_seq.size();i++){
         String this_kmer_seq=kmer_seq.get(i);
          String corr_seq="";
                        for(int j=0;j<this_kmer_seq.length();j++){
                            Character thisCharacter=this_kmer_seq.charAt(j);
                            String thisChar=thisCharacter.toString();
                            System.out.println("::::::::::::::::::::::"+thisChar);

                            if(thisChar.equals("A")){
                                corr_seq=corr_seq+"T";
                            }
                            if(thisChar.equals("T")){
                                corr_seq=corr_seq+"A";
                            }
                            if(thisChar.equals("C")){
                                corr_seq=corr_seq+"G";
                            }
                             if(thisChar.equals("G")){
                                corr_seq=corr_seq+"C";
                            }
                        }
                        
     }
             
      return(repeatKmersGrouped);
    }    
    
    public JavaRDD<String>  refineResult(JavaPairRDD<String,ArrayList<Integer>> rawResult ,int unitNum){
        // filtering out arrays having insufficient repat units
        final int min_repeat_unit=unitNum;
    
        JavaPairRDD<String,ArrayList<Integer>> result_have_enoughUnits= rawResult.filter(new Function<Tuple2<String,ArrayList<Integer>>, Boolean>(){
            @Override
            public Boolean call(Tuple2<String,ArrayList<Integer>> keyValue){
                ArrayList<Integer> unit_start_locs=keyValue._2();
                boolean isResultFine=true;
                if(unit_start_locs.size()<min_repeat_unit){
                     isResultFine=false;
                }
                return (isResultFine);
            }
        });

        JavaRDD<String> result_refined= result_have_enoughUnits.map(new ResultParser());

        return (result_refined);

    }


    class ResultParser implements Function<Tuple2<String,ArrayList<Integer>>,String> {
        @Override
        public String call(Tuple2<String,ArrayList<Integer>> seq_locs) {
          String result=null;
          // seq, rep_unit_num, crispr_start, crispr_end, spacer_start-spacer_end;spacer_start2-spacer-end2 
          String seq=seq_locs._1();
          ArrayList<Integer> locs=seq_locs._2();
          int crispr_start=locs.get(0);
          int crispr_end=locs.get(locs.size()-1)+seq.length()-1;
          String spacer_positions="";
          for(int i=0; i<locs.size()-1;i++){

            int this_sp_head=locs.get(i)+seq.length();
            int this_sp_end=locs.get(i+1)-1;

            spacer_positions=spacer_positions+this_sp_head+"-"+this_sp_end+";";

          }

          result=crispr_start+","+crispr_end+","+ spacer_positions;

          return(result);
        }

    }


    public JavaPairRDD<String, Integer> parseDevinOutput(JavaRDD<String> devin_output){
         JavaPairRDD <String, Integer> result=devin_output.flatMapToPair(new PairFlatMapFunction<String,String,Integer>(){
            @Override
            public Iterable<Tuple2<String,Integer>> call(String line){
                ArrayList<Tuple2<String, Integer>> parse_result = new ArrayList<Tuple2<String, Integer>> ();
                String[] temp=line.replaceAll("[()]","").split("CompactBuffer");
                String[] front=temp[0].split(",");
                String[] back=temp[1].split(",");
                for(int j=0;j<back.length;j++){
                    String thisSeq=front[0];
                    int k = thisSeq.length();
                    int thisPosition=Integer.parseInt(back[j].replaceAll(" ",""));
                    // transform devin's poisition to right-across position
                    if(thisPosition<0){
                        thisPosition=(-1)*(Math.abs(thisPosition)-k);     
                    }
                    parse_result.add(new Tuple2<String,Integer>(thisSeq,thisPosition));

                }

                return(parse_result);
            }

        });
        
        return(result);

    }

    // output : {seq,[unit_1_start_pos,unit_2_start_pos]}
    public JavaPairRDD<String,ArrayList<Integer>>extractRepeatPairCandidate(JavaPairRDD<String, Integer>parsedMRSMRSresult,int max_spacer_size, int min_spacer_size,int unit_length){
       final int min_search_range=min_spacer_size;
       final int max_search_range=2*max_spacer_size-unit_length;
       
       JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=parsedMRSMRSresult.groupByKey();
       JavaPairRDD<String,ArrayList<Integer>> result= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,String,ArrayList<Integer>>(){
                @Override
                public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<Integer>> keyValue){
                 Iterable<Integer>data =keyValue._2();
                 Iterator<Integer> itr=data.iterator();
                 String seq=keyValue._1();
                 ArrayList<Integer> locs_on_postiveStrand=new ArrayList<Integer>();
                 ArrayList<Tuple2<String, ArrayList<Integer>>> possibleRepeatUnits = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();

                 while(itr.hasNext()){
                   int thisLoc=itr.next();
                   if(thisLoc>0){
                      locs_on_postiveStrand.add(thisLoc);
                   }
                 }

                 Collections.sort(locs_on_postiveStrand);
                 int iterationNum=locs_on_postiveStrand.size();
                 for(int j=0;j<iterationNum;j++){
                     int thisPosLoc=locs_on_postiveStrand.get(j);
                     
                     if(j<iterationNum-1){
                        if(j<iterationNum-2){
                          int nextTwoPosLoc=locs_on_postiveStrand.get(j+2); 
                          int secondDist_pos=nextTwoPosLoc-thisPosLoc;
                          if(secondDist_pos<max_search_range && secondDist_pos>min_search_range){
                            ArrayList<Integer> thisPositionSet=new ArrayList<Integer>();
                            thisPositionSet.add(thisPosLoc);
                            thisPositionSet.add(nextTwoPosLoc);
                            possibleRepeatUnits.add(new Tuple2<String,ArrayList<Integer>>(seq,thisPositionSet));
                          }
                            
                        }
                        int nextPosLoc=locs_on_postiveStrand.get(j+1);
                        int firstDist_pos=nextPosLoc-thisPosLoc;
                        if(firstDist_pos<max_search_range && firstDist_pos>min_search_range){
                         ArrayList<Integer> thisPositionSet=new ArrayList<Integer>();
                         thisPositionSet.add(thisPosLoc);
                         thisPositionSet.add(nextPosLoc);
                         possibleRepeatUnits.add(new Tuple2<String,ArrayList<Integer>>(seq,thisPositionSet));
                            
                        }
                        
                  
                     }
               
                 }

                 return(possibleRepeatUnits);

                }

            });

        return(result);
   
    }

    // specifically designed for palindromic structuer  inside repeat unit
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
                        if(firstDist_pos<200 && secondDist_pos<200){
                            int iterationNum_neg=locs_on_negStrand.size();
                            for(int k=0;k<iterationNum_neg;k++){
                                int thisNegLoc=locs_on_negStrand.get(k);
                                if(k<iterationNum_neg-2){
                                    int firstDist_neg=locs_on_negStrand.get(k+1)-thisNegLoc;
                                    int secondDist_neg=locs_on_negStrand.get(k+2)-locs_on_negStrand.get(k+1);
                                    if(firstDist_neg<200 && secondDist_neg<200){
                                        int size=thisNegLoc-thisPosLoc; // size is referred to distance between head of left arm and tail of right arm
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

    //  grab all the possible imperfect palindromic structure with each arm is a kmer 
    //  need to consider merging later  to extend  arm in case of adjacent arms  or overlap arms across 
    // differnt kmers
    public  JavaPairRDD <String, ArrayList<Integer>> fetchImperfectPalindromeAcrossGenomes(JavaPairRDD <String, Integer> parsedMRSMRSresult,int armlen){
        
           final  int arm_len=armlen;
   
           JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=parsedMRSMRSresult.groupByKey();

            JavaPairRDD<String, ArrayList<Integer>> result= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,String, ArrayList<Integer>>(){
                @Override
                public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<Integer>> keyValue){
                 Iterable<Integer>data =keyValue._2();
                 String kmer_seq=keyValue._1();
                 Iterator<Integer> itr=data.iterator();
                 ArrayList<Integer> locs_on_postiveStrand=new ArrayList<Integer>();
                 ArrayList<Integer> locs_on_negStrand=new ArrayList<Integer>();
                 ArrayList<Tuple2<String, ArrayList<Integer>>> imperfect = new ArrayList<Tuple2<String, ArrayList<Integer>>> (); 
                 
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
                     int thisPosStartLoc=locs_on_postiveStrand.get(j);
                     int iterationNum_neg=locs_on_negStrand.size();
                     for(int k=0;k<iterationNum_neg;k++){
                         int thisNegStartLoc=locs_on_negStrand.get(k);
                         int junction_distance=thisNegStartLoc-(thisPosStartLoc+arm_len-1);
                         if(junction_distance>1){
                            ArrayList<Integer> posStart_junctionDistance=new ArrayList<Integer>();
                            posStart_junctionDistance.add(thisPosStartLoc);
                            posStart_junctionDistance.add(junction_distance);
                            imperfect.add(new Tuple2<String,ArrayList<Integer>>(kmer_seq,posStart_junctionDistance));
                         } 
                     }

                 }
               
                 // int iterationNum=locs_on_postiveStrand.size();
                 // for(int j=0;j<iterationNum;j++){
                 //     int thisPosStartLoc=locs_on_postiveStrand.get(j);
                 //     if(j<iterationNum-1){
                 //        int thisPosEndLoc=thisPosStartLoc+arm_len;
                 //        int nextPosStartLoc=locs_on_postiveStrand.get(j+1);
                        

                        // for merging adjacent and overlap arms to extend 
                        // boolean is_pos_overlap=false;
                        // boolean is_pos_adjacent=false;

                        // int dist_two_pos_loc=nextPosStartLoc-thisPosEndLoc;
                        
                        // if(dist_two_pos_loc==0){
                        //   is_pos_adjacent=true;
                        // }

                        // if(dist_two_pos_loc<0){
                        //   is_pos_overlap=true;
                        // }



                        // int iterationNum_neg=locs_on_negStrand.size();
                        // for(int k=0;k<iterationNum_neg;k++){
                        //         int thisNegLoc=locs_on_negStrand.get(k);
                        //         if(k<iterationNum_neg-1){
                        //             int dist_two_neg=locs_on_negStrand.get(k+1)-thisNegLoc;
                        //           // for merging adjacent and overlap arms to extend 
                        //           boolean is_neg_overlap=false;
                        //           boolean is_neg_adjacent=false;

                        
                        //           if(dist_two_neg==0){
                        //             is_neg_adjacent=true;
                        //           }
          
                        //           if(dist_two_neg<0){
                        //            is_neg_overlap=true;
                        //           }


                             
                              

                                          
                        //         }
                           
                        //     }
                            
             

                 return(imperfect);

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
                    // if(left_arm_starts.contains(j) && right_arm_starts.contains(j)){
                    //     containArm=true;
                    // }

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

                
                ArrayList<Tuple2<String, ArrayList<Integer>>> result = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                ArrayList<Integer> reasonableRepeatsLocs=new ArrayList<Integer>();
                if(repeat_locs.size()>1){
                    Collections.sort(repeat_locs);
                    for(int j=0;j<repeat_locs.size(); j++){
                        if(j!=repeat_locs.size()-1){
                             int thisMrsMrsLoc=repeat_locs.get(j);
                             int distance_between_units=repeat_locs.get(j+1)-thisMrsMrsLoc;
                             if(distance_between_units<=300){
                                reasonableRepeatsLocs.add(thisMrsMrsLoc);
                             }

                             else{
                                reasonableRepeatsLocs.add(thisMrsMrsLoc);
                                break;
                             }
                        }
                    }
                    result.add(new Tuple2<String, ArrayList<Integer>>(keyValue._1(),reasonableRepeatsLocs));

                }
                

                return(result);

            }


        });


        return(cirsprs);
    }


}

