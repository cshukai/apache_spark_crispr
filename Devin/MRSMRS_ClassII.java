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
    import java.io.BufferedReader;
    import java.io.IOException;
    import java.io.InputStreamReader;
    import java.io.FileReader;
    import org.apache.spark.storage.StorageLevel;
    
    public class MRSMRS implements Serializable{
    	public static void main(String [ ] args) throws Exception{
            /*env configuration*/
            SparkConf conf=new SparkConf();
        	JavaSparkContext sc=new JavaSparkContext(conf);   
            MRSMRS mrsmrs=new MRSMRS();
            String home_dir="/idas/sc724";
            /*user input*/
            
            //array structure
            int repeat_unit_min=15;
            int repeat_unit_max=70;
            int spacerMax=15;
            int spacerMin=90;
            
            //stem loop assocaited structures
            int stemLoopArmLen=4; // this is the minimal arm length of imperfect palindromes for internal stem loop
            int externalMaxStemLoopArmLen=30;
            int loopLowBound=3;
            int loopUpBound=8;
            double tracrAlignRatio=0.7; // in terms of proportion of length of max repeat unit
            int externalMaxGapSize=2; // distance between external imperfect palindrome and anti-repeat regio

            
            
            /*processing*/
            // input directories to gneerate buildinb block
           String species_folder=args[0]; //ex: "Streptococcus_thermophilus_lmd_9.GCA_000014485.1.29.dna.chromosome.Chromosome.fa";
           String fasta_path=home_dir+"/"+args[1];//ex:"Streptococcus_thermophilus_lmd_9.fa.txt";
           // String fasta_path=args[1];             
            
            // search of palindrome building block 
           JavaRDD<String> kBlock4PalindromeArms=sc.textFile(home_dir+"/"+stemLoopArmLen+"/"+species_folder);  

           JavaPairRDD<String,Integer>palindromeInput=mrsmrs.parseDevinOutput(kBlock4PalindromeArms);
          // JavaPairRDD<String,Integer>repeatUnitMers=mrsmrs.parseDevinOutput(buildingblock);
           
           palindromeInput.saveAsTextFile(home_dir+"/"+"mrsmrs");
           JavaPairRDD <String, ArrayList<Integer>>  palindBlock=mrsmrs.ImperfectPalindromeAcrossGenomes(palindromeInput,stemLoopArmLen,loopLowBound,loopUpBound);
           palindBlock.saveAsTextFile(home_dir+"/palindrome");
            
    /////////////       
            //mri
            List<String> fasta_temp=sc.textFile(fasta_path).collect();
            String fasta="";
            for(int i=0; i< fasta_temp.size();i++){
                fasta=fasta+fasta_temp.get(i);
            }

            /* external palindromes*/
            JavaPairRDD<String, ArrayList<Integer>> test5=mrsmrs.extractTracrTrailCandidate( palindBlock,90, 15, 75,15,2,fasta,15, externalMaxStemLoopArmLen);
            test5.saveAsTextFile(home_dir+"/crispr_test2");
            JavaPairRDD<Integer, ArrayList<String>> test6= mrsmrs.findMinimalTrailingArray(fasta,test5,palindromeInput,90 ,15 ,75,15,tracrAlignRatio,15,100);
            test6.saveAsTextFile(home_dir+"/crispr_test3");

    	}        
        

        /*
        
         names: 
         armer : k mer- repeat that is used to identify palindromic strucure 
          so armer are repeat seqeunce not neccessarily palidnromic arms
        
         algorithm:
            1. break down each trailing candidates into fragment 
               as long as minimum palindrome arm
            2. if a particular trailing candidate map a set of 
               palindrome arm-mer arrray that meet 
               tracr-alingment ratio, then select this trailing
               candidate
               
            
          output: { selected trailing sequence_startLoc of this trailing seq , start locations of units in a array  } 
        */
        public JavaPairRDD <Integer, ArrayList<String>> findMinimalTrailingArray(String fasta,JavaPairRDD <String, ArrayList<Integer>> trailingCandidate , JavaPairRDD<String, Integer> arm_mer,int spacerMaxLen, int spacerMinLen, int unitMaxLen,int unitMinLen, double tracrAlignRatio, int lengthOfTrailingSeq,int bucketScanSize) {
                final int r_max=unitMaxLen;
                final int r_min=unitMinLen;
                final int s_max=spacerMaxLen;
                final int s_min=spacerMinLen;
                final int unitDistMin=unitMinLen+spacerMinLen;
                final int unitDistMax=unitMaxLen+spacerMaxLen;
                final int trailingLen=lengthOfTrailingSeq;
                final double tracrAlignRatio2=tracrAlignRatio;
                final int bucketWindowSize=bucketScanSize;
                JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=arm_mer.groupByKey();
                JavaPairRDD<Integer,ArrayList<String>> selectedArmMer= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,Integer,ArrayList<String>>(){
                    @Override
                    public Iterable<Tuple2<Integer,ArrayList<String>>> call(Tuple2<String, Iterable<Integer>> keyValue){
                     Iterable<Integer>data =keyValue._2();
                     Iterator<Integer> itr=data.iterator();
                     String seq=keyValue._1();
                     ArrayList<Integer> locs_on_postiveStrand=new ArrayList<Integer>();
                     ArrayList<Tuple2<Integer, ArrayList<String>>> possibleRepeatUnits = new ArrayList<Tuple2<Integer, ArrayList<String>>> ();
    
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
                              int nextPosLoc=locs_on_postiveStrand.get(j+1); 
                              int firstDist=nextPosLoc-thisPosLoc;
                              
                              int secondDist_pos=nextTwoPosLoc-nextPosLoc;
                              if(secondDist_pos<=unitDistMax && secondDist_pos>=unitDistMin){
                                  if(firstDist<=unitDistMax && firstDist>=unitDistMin){
                                       ArrayList<String> thisPositionSet=new ArrayList<String>();
                                       thisPositionSet.add(seq);
                                       int bucketNum=(int)Math.ceil((thisPosLoc+nextPosLoc+nextTwoPosLoc)/(3*bucketWindowSize));
                                       thisPositionSet.add(Integer.toString(thisPosLoc));
                                       thisPositionSet.add(Integer.toString(nextPosLoc));
                                       thisPositionSet.add(Integer.toString(nextTwoPosLoc));
                                       possibleRepeatUnits.add(new Tuple2<Integer,ArrayList<String>>(bucketNum,thisPositionSet));    
                                      
                                  }
                               
                              }
                                
                            }
                            
                                
                            }
                            
                      
                         }
                   
                     
    
                     return(possibleRepeatUnits);
    
                    }
    
                });
             /*
                1.purpose : use k-mer array to assemble crispr array
                2.rationale :
                (1)if your two k-mer are within the same repeat unit , you can rest assure correspondingly trailing k-mers are  to be in the second/ third repeat units ,based on the  distance filter above 
                (2)determination of crispr array of class-II crispr array also depends on alingment against tracrRNA's anti-repeat region in addition to  spacer distance, so at this point, you just need to consider minimum crispr array with sequence variation, no need to consider truncated case in this point
                3.output:  goodRepeatUnitPairs <seq(repeat_unit),[size(spacer1),size(spacer2),unit1_start,unit1_end,unit2_start,unit2_end]
                          use 'N' to represent sequence variation
             */    
             
             JavaPairRDD<Integer,Iterable<ArrayList<String>>> kmersInSameBucket=selectedArmMer.groupByKey();
             
             JavaPairRDD<String,ArrayList<Integer>> goodRepeatUnitPairs = kmersInSameBucket.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<ArrayList<String>>>,String,ArrayList<Integer>>(){
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<Integer, Iterable<ArrayList<String>>> keyValue){
                     Iterable<ArrayList<String>> kmer_seq_starts =keyValue._2();
                     int  kmer_len=kmer_seq_starts.get(0).length();
                     Iterator<ArrayList<String>> itr=kmer_seq_starts.iterator();

                     ArrayList<Integer> kmer_arr_1st_pos= new ArrayList<Integer>();
                     ArrayList<Integer> kmer_arr_2nd_pos= new ArrayList<Integer>();
                     ArrayList<Integer> kmer_arr_3rd_pos= new ArrayList<Integer>();

                     while(itr.hasNext()){
                       ArrayList<String> this_kmer_seq_starts=itr.next();
                       for(int i=1;i<this_kmer_seq_starts.size();i++){
                           if(i==1){
                               kmer_arr_1st_pos.add(Integer.parseInt(this_kmer_seq_starts.get(i)));
                           }
                           if(i==2){
                               kmer_arr_2nd_pos.add(Integer.parseInt(this_kmer_seq_starts.get(i)));
                           }
                           if(i==3){
                               kmer_arr_3rd_pos.add(Integer.parseInt(this_kmer_seq_starts.get(i)));;
                               
                           }
                       }

                     }
                     
                     ArrayList<Integer> kmer_arr_1st_pos_nonSorted=kmer_arr_1st_pos;// index for retrival later since sorting disrupt original order
                     Collections.sort(kmer_arr_1st_pos);
                     ArrayList<Integer> indexes= new ArrayList<Integer>(); // store the index for the valid k-mers pairs with reasonable distance in between
                     
                     ArrayList<Integer>repeat_unit_locs=new ArrayList<Integer>(); // for storage of start and end locations of 3 units of a crispr arra
                     String consensus_seq="";
                     ArrayList<Tuple2<String, ArrayList<Integer>>> output = new ArrayList<Tuple2<String, ArrayList<Integer>>> (); 
                     for(int i=0;i<kmer_arr_1st_pos.size()-1;i++){
                             int j=i+1;
                             int this_1st_pos=kmer_arr_1st_pos.get(i); 
                             int this_1st_end=this_1st_pos+kmer_len-1;
                             int next_1st_pos=kmer_arr_1st_pos.get(j);
                            
                             int first_dist=next_1st_pos-this_1st_pos;
                             
                             // take care of first repeat unit and location of other units can be estimated 
                             while(first_dis<=r_max-kmer && j<kmer_arr_1st_pos.size()){
                                 // check if the kmer in second/ third repeat units follow the same kmer-occurence order in the first repat unit
                                 int corresponding_first_idx=kmer_arr_1st_pos_nonSorted.indexOf(this_1st_pos);
                                 int corresponding_next_idx=kmer_arr_1st_pos_nonSorted.indexOf(next_1st_pos);
                                 
                                 int this_2nd_pos=kmer_arr_2nd_pos.get(corresponding_first_idx); 
                                 int next_2nd_pos=kmer_arr_3rd_pos.get(corresponding_next_idx);
                                 int second_dis=next_2nd_pos-this_2nd_pos;
                                 if(second_dis==first_dis){
                                     int this_3rd_pos=kmer_arr_3rd_pos.get(corresponding_first_idx);
                                     int next_3rd_pos=kmer_arr_3rd_pos.get(corresponding_next_idx);
                                     int third_dis=next_3rd_pos-this_3rd_pos;
                                     if(third_dis==first_dist){
                                         // order and distance are the same , so sorting in first unit guratnee kmers in second/third unit are sorted by location
                                         // start to figure out the consensus seqeunce
                                         //  if nth units between two k-me arrays are adjacent/overlap to each other , then merge , otherwise use "N" to represent sequence variance with repeat unit
                                         int dist_kmers_in_unit=this_2nd_pos-this_1st_pos+1;
                                         int merge_cutoff=this_1st_end-this_1st_pos+1;
                                         this_2nd_end=this_2nd_pos+kmer_len-1;
                                         if(dist_kmers_in_unit<=merge_cutoff){ // adjacent/overlap case , need to update i after merging the two -kmer array 
                                        
                                             if(repeat_unit_locs.size()==0){ // add the start position of very first copy
                                                repeat_unit_locs.add(this_1st_pos); 
                                             }
                                             if(repeat_unit_locs.size()==1){ // add the end position of second copy
                                                repeat_unit_locs.add(this_2nd_end); 
                                             }
                                             if(repeat_unit_locs.size()>1){ // already extended once, continue to extend by updating with the end position of newly added copy
                                                repeat_unit_locs.set(repeat_unit_locs.size()-1,this_2nd_end);    
                                             }
                                             consensus_seq=fasta.substring(this_1st_pos,this_2nd_end);
                                             break;
                                         }
                                         
                                         else{// seq variance case, use "N" for representation
                                               int spaceBetween=this_2nd_pos-this_1st_end+1;
                                               if(repeat_unit_locs.size()==0){ // add the start position of very first copy
                                                repeat_unit_locs.add(this_1st_pos); 
                                               }
                                               
                                         }
                                         
                                     }
                                 }
                              j=j+1;
                             }
                     }
                    
                   


                     return(output);
    
                    }
             });
             return(goodRepeatUnitPairs);
//             // use arm-mer with CRISPR-like architecture to select trailing seqeunce by matching
//             // output : {matched_arm_seq, [trailingSeq_trailingLocation,matchedOrder]}
//             JavaPairRDD<String ,ArrayList<String>> trailingSeq_matchedArmer = trailingCandidate.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<String>>(){
//                     @Override
//                     public Iterable<Tuple2<String,ArrayList<String>>> call(Tuple2<String, ArrayList<Integer>> keyValue){
//                       ArrayList<Integer> tracrRelatedLocs =keyValue._2();
//                       int trailingStart=tracrRelatedLocs.get(2);
//                       String trailing_seq=keyValue._1();
//                       String thisTrailingInfo=trailing_seq+"_"+trailingStart;
//                       ArrayList<Tuple2<String, ArrayList<String>>> result1 = new ArrayList<Tuple2<String, ArrayList<String>>> ();
//                       ArrayList<String> theseMatchedArms= new ArrayList<String>();

//                       for(int i=0;i<trailingLen;i++){
//                           if(i<trailingLen-armlen){
//                             String thisWindowSeqTemp=trailing_seq.substring(i,i+armlen);
//                             String thisWindowSeq="";// same strand 
                           
//                             for(int j=0;j<thisWindowSeqTemp.length();j++){
//                                 Character thisChar=thisWindowSeqTemp.charAt(j);
//                                 String  thisAlpha=thisChar.toString();
//                                 if(thisAlpha.equals("A")){
//                                   thisWindowSeq=thisWindowSeq+"T";                                
//                                 }
                                
//                                 if(thisAlpha.equals("T")){
//                                   thisWindowSeq=thisWindowSeq+"A";                                
//                                 }
                                
//                                 if(thisAlpha.equals("C")){
//                                   thisWindowSeq=thisWindowSeq+"G";                                
//                                 }
                                
//                                 if(thisAlpha.equals("G")){
//                                   thisWindowSeq=thisWindowSeq+"C";                                
//                                 }
                                
//                             }
                           
                           
//                             String thisWindowSeqRev=""; // different strand 
//                             for(int j=thisWindowSeq.length()-1;j>=0;j--){
//                                 Character thisChar=thisWindowSeq.charAt(j);
//                                 thisWindowSeqRev=thisWindowSeqRev+thisChar.toString();
                                
//                             }
                            
                           
                            
                            
//                             if(selectedArmSeq2.contains(thisWindowSeq)){
//                                 ArrayList<String> temp= new ArrayList<String>();
//                                 temp.add(thisTrailingInfo);
//                               // result1.add(new Tuple2<String, ArrayList<String>>(thisWindowSeq,temp));
                            
//                                 result1.add(new Tuple2<String, ArrayList<String>>(selectedArmSeq2.get(selectedArmSeq2.indexOf(thisWindowSeq)),temp));
//                             }
                            
                            
                            
//                             if(selectedArmSeq2.contains(thisWindowSeqRev)){
//                                 ArrayList<String> temp= new ArrayList<String>();
//                                 temp.add(thisTrailingInfo);
//                                 //result1.add(new Tuple2<String, ArrayList<String>>(thisWindowSeqRev,temp));

//                                 result1.add(new Tuple2<String, ArrayList<String>>(selectedArmSeq2.get(selectedArmSeq2.indexOf(thisWindowSeqRev)),temp));

//                             }
//                           }
//                       }

                      
//                      return(result1);
//                     }
//                 });


//             // you can proabably sort the smaller kmer by location to reduce the future search space             
//             JavaPairRDD <String,Tuple2<ArrayList<String>,ArrayList<String>>> mashup=trailingSeq_matchedArmer.join(selectedArmMer).repartition(3000); //this line needs to be modfied  as you can consider overlap of k-mer arrays  to reduce search space
            
//             mashup.persist(StorageLevel.MEMORY_AND_DISK());
    

// //            trailingSeq_matchedArmer.saveAsTextFile("trailin_matchtest");
// //            System.out.println("trailingseq:"+trailingSeq_matchedArmer.count());
// //            System.out.println("mashup:"+mashup.count());
//             // output format {seqOfTraing, [trailingstart, matchorder, arm_array_unit_starts]}
//             JavaPairRDD <String, ArrayList<Integer>> mashup2= mashup.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<ArrayList<String>,ArrayList<String>>>,String,ArrayList<Integer>>(){
//                      @Override
//                      public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,Tuple2<ArrayList<String>,ArrayList<String>>> keyValue){
//                          ArrayList<Tuple2<String, ArrayList<Integer>>> output2 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  
//                          ArrayList<Integer>locs=new ArrayList<Integer>();
//                          Tuple2<ArrayList<String>,ArrayList<String>> temp=keyValue._2();
                         
//                          String[] temp2=temp._1().get(0).split("_");
//                          int thisTrailingStart=Integer.parseInt(temp2[1]);
//                          locs.add(thisTrailingStart);
//                          String tracrSeq=temp2[0];
                         
//                          for(int i=0;i<temp._2().size();i++){
//                             locs.add(Integer.parseInt(temp._2().get(i)));
//                          }
              
//                         output2.add(new Tuple2<String, ArrayList<Integer>>(tracrSeq,locs));

//                         return(output2); 
//                      }
//                 }).repartition(3000);
//             mashup2.persist(StorageLevel.MEMORY_AND_DISK());
            
//             JavaPairRDD <String ,Iterable<ArrayList<Integer>>> mashup3=mashup2.groupByKey();
//             //        System.out.println("mashup3:"+mashup3.count());

//             JavaPairRDD <String, ArrayList<Integer>>result=mashup3.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<ArrayList<Integer>>>,String,ArrayList<Integer>>(){
//                     @Override
//                     public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,Iterable<ArrayList<Integer>>> keyValue){
//                         String thisTrailingSeq=keyValue._1();
//                         Iterable<ArrayList<Integer>> matchInfo =keyValue._2();
//                         Iterator <ArrayList<Integer>> itr=matchInfo.iterator();
//                         ArrayList<Tuple2<String, ArrayList<Integer>>>output3 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                        
//                         String tracrStarts="";
//                         ArrayList<Integer> firstUnitStarts =new ArrayList<Integer>();
//                         ArrayList<Integer> secondUnitStarts =new ArrayList<Integer>();
//                         ArrayList<Integer> thirdUnitStarts =new ArrayList<Integer>();
                  
//                         while(itr.hasNext()){
//                           ArrayList<Integer> thisMacthInfo=itr.next();  
                          
//                           tracrStarts=tracrStarts+"_"+thisMacthInfo.get(0); // collecting  start locations of one trailing seq 
//                           firstUnitStarts.add(thisMacthInfo.get(1));
//                           secondUnitStarts.add(thisMacthInfo.get(2));
//                           thirdUnitStarts.add(thisMacthInfo.get(3));
                             
            
//                         }            
                        
//                          ArrayList<Integer>firstUnitStartsSorted=firstUnitStarts;
//                          Collections.sort(firstUnitStartsSorted);
//                          ArrayList<Integer>filterdStarts=new ArrayList<Integer>();
//                          // merging arm-mer which  can form a minimal array and reside in the same repeat unit
//                          for(int i=0;i<firstUnitStartsSorted.size()-1;i++){
//                               int thisFirstStart=firstUnitStartsSorted.get(i);
//                               int nextFirstStart=firstUnitStartsSorted.get(i+1);
//                               int interval=nextFirstStart-thisFirstStart;
//                               if(interval>=armlen && interval<r_max-armlen){
//                                   if(!filterdStarts.contains(thisFirstStart)){
                                      
//                                       int  secondThisStart=secondUnitStarts.get(firstUnitStarts.indexOf(thisFirstStart));
//                                       int  thirdThisStart=thirdUnitStarts.get(firstUnitStarts.indexOf(thisFirstStart));
                                      
//                                       int  thisFirstDist=secondThisStart-thisFirstStart;
//                                       int  thisSecondDist=thirdThisStart-secondThisStart;
                                      
//                                       if(thisFirstDist>=r_min && thisSecondDist>=r_min){
//                                           if(thisSecondDist<=r_max && thisFirstDist <=r_max){
//                                              filterdStarts.add(thisFirstStart);
//                                              filterdStarts.add(secondThisStart);
//                                              filterdStarts.add(thirdThisStart);
                                              
//                                           } 
//                                       }
                    
//                                         int  secondNextStart=secondUnitStarts.get(firstUnitStarts.indexOf(nextFirstStart));
//                                         int  thirdNextStart=thirdUnitStarts.get(firstUnitStarts.indexOf(nextFirstStart));
//                                         int  nextFirstDistance=secondNextStart-nextFirstStart;
//                                         int  nextSecondDistance=thirdNextStart-secondNextStart;                           
//                                         if(nextFirstDistance>=r_min && nextSecondDistance>=r_min){
//                                             if(nextFirstDistance<=r_max && nextSecondDistance<=r_max){
//                                                 filterdStarts.add(nextFirstStart);
//                                                 filterdStarts.add(secondNextStart);
//                                                 filterdStarts.add(thirdNextStart);
//                                             }
//                                         }       
                                          
//                                   }
//                               }
//                          }
//                       String outKey=thisTrailingSeq+tracrStarts;        
//                       output3.add(new Tuple2<String, ArrayList<Integer>>(outKey,filterdStarts));     
//                       return(output3);   
//                     }
                        
//             });     

//            return(result);        
        }
        
        
            private List<String> readFile(String filename) throws Exception {
                String line = null;
                List<String> records = new ArrayList<String>();
     
                // wrap a BufferedReader around FileReader
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
     
                // use the readLine method of the BufferedReader to read one line at a time.
                // the readLine method returns null when there is nothing else to read.
                while ((line = bufferedReader.readLine()) != null){
                      records.add(line);
                     }
       
                // close the BufferedReader when we're done
                bufferedReader.close();
                return records;
              }
        
        
        
        /* purpose: extraction of trailing part of tracrRNA for further matching with MRSRMSR k mer
           assumption: assuming  every palindorm previously identified
           is a part of  tracRNA
           input :  output of ImperfectPalindromeAcrossGenome
           output: key-value pairs
           {[seq(trailing_seq) ], [start_pos(palindrome),end_pos(palindrome),start_pos(tralingSeq),end_pos(tralingSeq)]}
           algorithm: sequence alginme
           mrsmrs
        */
        public  JavaPairRDD <String, ArrayList<Integer>>  extractTracrTrailCandidate( JavaPairRDD <String, ArrayList<Integer>> palindBlock, int spacerMaxLen, int spacerMinLen, int unitMaxLen,int unitMinLen,int externalMaxGapSize,String fastaFile,int lengthOfTrailing,int externalMaxStemLoopArmLen){
                final int r_max=unitMaxLen;
                final int r_min=unitMinLen;
                final int s_max=spacerMaxLen;
                final int s_min=spacerMinLen;
                final int armLenMax=externalMaxStemLoopArmLen;
                final int gap_size=externalMaxGapSize;
                final String fasta=fastaFile;
                final int lengthOfTrailingSeq=lengthOfTrailing;
                
                
                JavaPairRDD <String, ArrayList<Integer>> result =palindBlock.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<Integer>>(){
                     @Override
                     public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, ArrayList<Integer>> keyValue){
                         ArrayList<Tuple2<String, ArrayList<Integer>>> output2 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  
                         
                         ArrayList<Integer> temp=keyValue._2();  
                         String[] temp2=keyValue._1().split(":");
                         
                          // retreive the start and end location of palindrome for extension
                         int thisPalinStar=temp.get(0);
                         int loopLen=temp.get(1);
                         String armSeq=temp2[0];
                         int armLen=armSeq.length();
                         int thisPalinEnd=thisPalinStar+2*armLen+loopLen-1;
                         
                         // extension to find the longest possible palindromic arms within specified region
                         int numOfExtraBasesEachSide=armLenMax-armLen;
                         int leftStart=thisPalinStar-numOfExtraBasesEachSide-1;
                         if(leftStart<=0){
                             leftStart=0;
                         }
                         int leftEnd=thisPalinStar-2;
                         if(leftEnd<=0){
                             leftEnd=0;
                         }
                         
                         
                        String leftSuspect=fasta.substring(leftStart,leftEnd);
                             
                         
                         
                         
                         
                         int endPositionOfRightSuspect=thisPalinEnd+numOfExtraBasesEachSide-1;
                         String rightSuspect="";
                         if(endPositionOfRightSuspect>fasta.length()){
                             leftSuspect=""; 
                         }
                         
                         
                         else{
                             rightSuspect=fasta.substring(thisPalinEnd,endPositionOfRightSuspect);
                         }
                       
                         for(int i=0;i<leftSuspect.length();i++){
                             
                             Character rightChar=rightSuspect.charAt(i);
                             
                             Character leftChar=leftSuspect.charAt(leftSuspect.length()-i-1);
                             String thisRightBase=rightChar.toString();
                             String thisLeftBase=leftChar.toString();
                             if(thisRightBase.equals("A") && thisLeftBase.equals("T")){
                                 thisPalinStar=thisPalinStar-1;
                                 thisPalinEnd=thisPalinEnd+1;
                             }
                             if(thisRightBase.equals("C") && thisLeftBase.equals("G")){
                                 thisPalinStar=thisPalinStar-1;
                                 thisPalinEnd=thisPalinEnd+1;
                             }
                             
                             if(thisRightBase.equals("G") && thisLeftBase.equals("C")){
                                 thisPalinStar=thisPalinStar-1;
                                 thisPalinEnd=thisPalinEnd+1;
                             }
                             
                             if(thisRightBase.equals("T") && thisLeftBase.equals("A")){
                                 thisPalinStar=thisPalinStar-1;
                                 thisPalinEnd=thisPalinEnd+1;
                             }
                             
                         }
                         
                         //formation of comprehensive list of candidate of trailing sequence
                         
                         for(int i=1;i<=gap_size;i++){
                             
                             if(thisPalinEnd+i-1 <=fasta.length() && thisPalinEnd+i+lengthOfTrailingSeq-2 <=fasta.length() ){
                                String thisRightTrail=fasta.substring(thisPalinEnd+i-1,thisPalinEnd+i+lengthOfTrailingSeq-1);
                                ArrayList<Integer> locations= new ArrayList<Integer>();                                                
                                locations.add(thisPalinStar);
                                locations.add(thisPalinEnd);
                                locations.add(thisPalinEnd+i);
                                locations.add(thisPalinEnd+i+lengthOfTrailingSeq-1);
                                output2.add(new Tuple2<String, ArrayList<Integer>>(thisRightTrail,locations));

                             }
                             
                             else{
                                 break;
                             }
                             
                             if(thisPalinStar-i-lengthOfTrailingSeq >=1 &&thisPalinStar-i-1 >=1){
                                ArrayList<Integer> locations= new ArrayList<Integer>();                                                
                                String thisLeftTrail=fasta.substring(thisPalinStar-i-lengthOfTrailingSeq-1,thisPalinStar-i-1);
                                locations.add(thisPalinStar);
                                locations.add(thisPalinEnd);
                    
                                locations.add(thisPalinStar-i-lengthOfTrailingSeq-1);
                                locations.add(thisPalinStar-i);
                                    
                                output2.add(new Tuple2<String, ArrayList<Integer>>(thisLeftTrail,locations));

                             }
                             else{
                                 break;
                             }

                         }
                         
                         
                        return(output2); 
                     }
                });
                    
            return(result );            
                
        }

        /* key: unit seq
           values: unit starts*/
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
                     Collections.sort(locations);
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
        public  JavaPairRDD <String, ArrayList<Integer>> ImperfectPalindromeAcrossGenomes(JavaPairRDD <String, Integer> parsedMRSMRSresult,int armlen, int loop_size_min, int loop_size_max){
            
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
                   //System.out.println(startLine);
                   //System.out.println(startLocIdxInLine);
                   //System.out.println(endLocIdxInLine);
                   
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
    
