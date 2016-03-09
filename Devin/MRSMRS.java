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
            
            /*user input*/
            
            //array structure
            int repeat_unit_min=20;
            int repeat_unit_max=50;
            int spacerMax=75;
            int spacerMin=20;
            
            //stemp loop 
            int stemLoopArmLen=4;
            int loopLowBound=3;
            int loopUpBound=8;
            double tracrAlignRatio=0.5;
            int externalMaxGapSize=2; // distance between external imperfect palindrome and alinged region
            
            
            
            
            //processing
            String home_dir="/result";
            String species_folder="Clostridium_kluyveri_dsm_555.GCA_000016505.1.29.dna.chromosome.Chromosome.fa";
            
            // search of palindrome building block 
            //JavaRDD<String> kBlock4PalindromeArms=sc.textFile(home_dir+"/"+stemLoopArmLen+"/"+species_folder);  
            //JavaPairRDD<String,Integer>palindromeInput=mrsmrs.parseDevinOutput(kBlock4PalindromeArms);
            //JavaPairRDD <String, ArrayList<Integer>>  palindBlock=mrsmrs.fetchImperfectPalindromeAcrossGenomes(palindromeInput,stemLoopArmLen,loopLowBound,loopUpBound);
            //JavaPairRDD <String,ArrayList<Integer>> test_3=mrsmrs.extractPalinDromeArray(palindBlock,75,20,50,20,4); 

            JavaRDD<String> kBlock_repeat_unit=sc.textFile("/result/20/Clostridium_kluyveri_dsm_555.GCA_000016505.1.29.dna.chromosome.Chromosome.fa");  
            JavaPairRDD<String,Integer> test=mrsmrs.parseDevinOutput(kBlock_repeat_unit);
            JavaPairRDD <String,ArrayList<Integer>> test_2=mrsmrs.extractRepeatPairCandidate(test,75,20,20);
            JavaPairRDD<String,ArrayList<Integer>>   test_4=mrsmrs.extractInsideStemLoopRepeatPairs(test_2, 0.5,4,3,8); 
            test_4.saveAsTextFile("crispr_test2");

            JavaPairRDD<String,ArrayList<Integer>>   test_5=mrsmrs.formArrayWithMinUnitLen(test_4);
            test_5.saveAsTextFile("crispr_test");
            //search of repeat building block on top of palindrome block
            /*
            for(int repeat_len=repeat_unit_min; repeat_len<=repeat_unit_max; repeat_len++){
                JavaRDD<String> kBlock_repeat_unit=sc.textFile(home_dir+"/"+repeat_len+"/"+species_folder);  
                JavaPairRDD<String,Integer> test=mrsmrs.parseDevinOutput(kBlock_repeat_unit);
                JavaPairRDD <String,ArrayList<Integer>> test_2=mrsmrs.extractRepeatPairCandidate(test,spacerMax,spacerMin,repeat_len);
                
            }
    */
    	}        
        
        /*todo: extend each unit to both end for detction of truncated cases
        */
        
        // key: unit seq
        //values: unit starts
        public JavaPairRDD<String,ArrayList<Integer>>  formArrayWithMinUnitLen(JavaPairRDD<String,ArrayList<Integer>> relevantRepeatPair){
            JavaPairRDD<String, Iterable<ArrayList <Integer>>>  pairDromes = relevantRepeatPair.groupByKey();
            JavaPairRDD<String,ArrayList<Integer>> result=pairDromes.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<ArrayList<Integer>>>,String,ArrayList<Integer>>(){
                    ArrayList<Integer> unit1_starts=new ArrayList<Integer>();
                    ArrayList<Integer> unit2_starts=new ArrayList<Integer>();
                    ArrayList<Tuple2<String, ArrayList<Integer>>> output = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<ArrayList<Integer>>> keyValue){
                     Iterable<ArrayList<Integer>> locs =keyValue._2();
                     Iterator <ArrayList<Integer>> itr=locs.iterator();
                   
                     while(itr.hasNext()){
                        ArrayList<Integer> thisLocSet=itr.next();
                        unit1_starts.add(thisLocSet.get(0));
                        unit2_starts.add(thisLocSet.get(1));
                     }
    
    
                     
                     for(int i =0; i<unit1_starts.size();i++){
                         int thisUnit1Start=unit1_starts.get(i); 
                         int idx=unit2_starts.indexOf(thisUnit1Start);
                         if(idx!=-1){
                             ArrayList<Integer> minArrLocs=new ArrayList<Integer>();
                             int arr_unit1_start=unit1_starts.get(idx);
                             int arr_unit2_start=unit2_starts.get(idx);
                             int arr_unit3_start=unit2_starts.get(i);
                             minArrLocs.add(arr_unit1_start);
                             minArrLocs.add(arr_unit2_start);
                             minArrLocs.add(arr_unit3_start);
                               //try to  extend minimum array to longest possible
                             int  j=0;
                             while(j<unit1_starts.size()){
                                 if(j!=i){
                                  int thisTargetStartLoc=unit1_starts.get(j);
                                  int thisTargetStartLoc2=unit2_starts.get(j);
                                  if(thisTargetStartLoc==arr_unit3_start){
                                     minArrLocs.add(thisTargetStartLoc2);
                                     arr_unit3_start=thisTargetStartLoc2;
                                   }
                                
                                 
                                 }
                                 j=j+1;    
                            }
                            output.add(new Tuple2<String, ArrayList<Integer>>(keyValue._1(),minArrLocs));

                         }
                             
                         
                           
                         }
                         
                         
                             
                     
                  
                
                     return(output);
    
                    }
    
                });
             
       
        JavaPairRDD<String, Iterable<ArrayList <Integer>>>  pairDromes2 =result.distinct().groupByKey();
        
        JavaPairRDD<String,ArrayList<Integer>> result2=pairDromes2.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<ArrayList<Integer>>>,String,ArrayList<Integer>>(){
                    
                    ArrayList<Tuple2<String, ArrayList<Integer>>> output2 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<ArrayList<Integer>>> keyValue){
                     Iterable<ArrayList<Integer>> locs =keyValue._2();
                     Iterator <ArrayList<Integer>> itr=locs.iterator();
                     ArrayList<Integer> locations=new ArrayList<Integer>();
                     while(itr.hasNext()){
                        ArrayList<Integer> thisLocSet=itr.next();
                        for(int i=0;i<thisLocSet.size();i++){
                            locations.add(thisLocSet.get(i));
                        }
                     }

                     output2.add(new Tuple2<String, ArrayList<Integer>>(keyValue._1(),locations));     
                     return(output2);
    
                    }
    
                });

          
          JavaPairRDD<String,ArrayList<Integer>> result3=result2.distinct();
          
          

         
          return(result3);        
        }

        //key : seq(repeat pair)
        //values : [start_loc_1_unit_1, start_loc_2_unit_2, arm_start_loc,arm_len,gap_size]
        public JavaPairRDD<String,ArrayList<Integer>> extractInsideStemLoopRepeatPairs(JavaPairRDD<String,ArrayList<Integer>> repeatPairs,double tracer_repeat_similarity,int min_arm_len,int min_loop_size,int max_loop_size){
            final int minLoopSize= min_loop_size;
            final int maxLoopSize= max_loop_size;
            final int arm_len= min_arm_len; 
            
            
            //output : {unit seqeunce:kmer sequence,[unit_start,singleKmer_start]}
            JavaPairRDD<String,ArrayList<String>> repeat_pair_kmers=repeatPairs.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<String>>(){
                    @Override
                    public Iterable<Tuple2<String,ArrayList<String>>> call(Tuple2<String,ArrayList<Integer>> keyValue){
                        String thisSeq=keyValue._1();
                        ArrayList<Integer> seq_starts=keyValue._2();
                        int first_portion_star=seq_starts.get(0);
                        int second_portion_star=seq_starts.get(1);
                        
                        ArrayList<Tuple2<String, ArrayList<Integer>>>result = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                        //to record sequce and start position of every record
                        // k in k mers equal to min arm length
                         List<Tuple2<String,ArrayList<String>>> kmer_list =new ArrayList<Tuple2<String,ArrayList<String>>>();
                        for(int i=0;i<thisSeq.length()-arm_len+1;i++){
                            
                            String unit_seq_loc=thisSeq+":"+first_portion_star+":"+second_portion_star;
                            
                            String kmer=thisSeq.substring(i,i+arm_len);
                            ArrayList<String> kmerSeqlocs=new ArrayList<String>();
                            int kmerLoc=i+first_portion_star;
                            String thisKmerSeqLoc=kmer+":"+kmerLoc;
                            kmerSeqlocs.add(thisKmerSeqLoc);
                            kmer_list.add(new Tuple2<String,ArrayList<String>>(unit_seq_loc,kmerSeqlocs));
                        }
                            return(kmer_list);
                        }
                        
                   });
                   
        JavaPairRDD<String, Iterable<ArrayList <String>>> repeatKmersGrouped = repeat_pair_kmers.groupByKey();
        JavaPairRDD<String,ArrayList<Integer>> result=repeatKmersGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<ArrayList<String>>>,String,ArrayList<Integer>>(){
                    
                    // output: unit that has stem-loop structure defined by user
                    // output format : { (unit_seq, unit_start), (left_arm_start,arm_len,loop_len)
                    ArrayList<Tuple2<String, ArrayList<Integer>>> output = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                    ArrayList<Integer>kmerLocation=new ArrayList<Integer>();
                    ArrayList<String>kmerSequence=new ArrayList<String>();
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<ArrayList<String>>> keyValue){
                     Iterable<ArrayList<String>> kmer_locs =keyValue._2();
                     Iterator <ArrayList<String>> itr=kmer_locs.iterator();
                     String unitSeq_loc=keyValue._1();
                     String[] tempArr=unitSeq_loc.split(":");
                     
                     while(itr.hasNext()){
                        ArrayList<String> thisKmerSequenceLocation=itr.next();
                        String [] tmp=thisKmerSequenceLocation.get(0).split(":");
                        kmerSequence.add(tmp[0]);
                        kmerLocation.add(Integer.parseInt(tmp[1]));
                     }
    
                  
                     for(int i=0;i<kmerSequence.size();i++){
                         String currentKmerSeq=kmerSequence.get(i);
                         String corrSeq="";
                         
                         for(int j=currentKmerSeq.length()-1;j>=0;j--){
                             Character thisTempChar=currentKmerSeq.charAt(j);
                             String thisChar=thisTempChar.toString();
                             if(thisChar.equals("A")){
                                 corrSeq=corrSeq+"T";
                             }
                             if(thisChar.equals("T")){
                                 corrSeq=corrSeq+"A";
                             }
                             if(thisChar.equals("C")){
                                 corrSeq=corrSeq+"G";
                             }
                             if(thisChar.equals("G")){
                                 corrSeq=corrSeq+"C";
                             }
                             
                         }
                         int currentKmerLocation=kmerLocation.get(i);
                         for(int k=0;k<kmerLocation.size();k++){
                             if(kmerSequence.get(k).equals(corrSeq) && kmerLocation.get(k)>currentKmerLocation){
                                
                                int loopsize=kmerLocation.get(k)-(currentKmerLocation+arm_len-1)-1;
                                if(loopsize>=minLoopSize &&  loopsize<=maxLoopSize){
                                     ArrayList<Integer> thisPalindrome=new ArrayList<Integer>();
                               
                                    String unit_sequence=tempArr[0];
                                    int unit_1_start_loc=Integer.parseInt(tempArr[1]);
                                    int unit_2_start_pos=Integer.parseInt(tempArr[2]);
                                    thisPalindrome.add(unit_1_start_loc);
                                    thisPalindrome.add(unit_2_start_pos);
                                    thisPalindrome.add(currentKmerLocation);
                                    thisPalindrome.add(arm_len);
                                    thisPalindrome.add(loopsize);
                                    output.add(new Tuple2<String, ArrayList<Integer>>(unit_sequence,thisPalindrome));     
                                }
                               
                             }
                         }
                         
                         
                     }
                     return(output);
    
                    }
    
                });
                
            JavaPairRDD<String,ArrayList<Integer>> result2=result.distinct();    
            return(result2);
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
           //final int max_search_range=2*max_spacer_size-unit_length;
           final int max_search_range=max_spacer_size+unit_length;
           
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
    
    
        //  grab all the possible imperfect palindromic structure with each arm is a kmer 
        //  need to consider merging later  to extend  arm in case of adjacent arms  or overlap arms across 
        // differnt kmers
        public  JavaPairRDD <String, ArrayList<Integer>> fetchImperfectPalindromeAcrossGenomes(JavaPairRDD <String, Integer> parsedMRSMRSresult,int armlen, int loop_size_min, int loop_size_max){
            
               final int arm_len=armlen;
               final int loopSizeMax= loop_size_max;
               final int loopSizeMin=loop_size_min;
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
                             int junction_distance=thisNegStartLoc-(thisPosStartLoc+arm_len-1)-1;
                             if(junction_distance>=loopSizeMin && junction_distance<= loopSizeMax){
                                ArrayList<Integer> posStart_junctionDistance=new ArrayList<Integer>();
                                posStart_junctionDistance.add(thisPosStartLoc);
                                posStart_junctionDistance.add(junction_distance);
                            
                                imperfect.add(new Tuple2<String,ArrayList<Integer>>(kmer_seq+":"+junction_distance,posStart_junctionDistance));
                             } 
                         }
    
                     }
                   
                                
                 
    
                     return(imperfect);
    
                    }
    
                });
    
            return(result);
    
                
    
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
      
    
    
    
    }
    
