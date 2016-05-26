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
    
    public class MRSMRS implements Serializable{
    	public static void main(String [ ] args) throws Exception{
            /*env configuration*/
            SparkConf conf=new SparkConf().setAppName("spark-crispr");
        	JavaSparkContext sc=new JavaSparkContext(conf);   
            MRSMRS mrsmrs=new MRSMRS();
            
            /*user input*/
            
            //array structure
            int repeat_unit_min=15;
            int repeat_unit_max=70;
            int spacerMax=15;
            int spacerMin=90;
            
            //stem loop assocaited structures
            int stemLoopArmLen=4; // this is the minimal arm length of imperfect palindromes for internal stem loop
            int externalMaxStemLoopArmLen=8;
            int loopLowBound=3;
            int loopUpBound=8;
            double tracrAlignRatio=0.7; // in terms of proportion of length of max repeat unit
            int externalMaxGapSize=2; // distance between external imperfect palindrome and alinged region

            
            
            /*processing*/
            // input directories to gneerate buildinb block
            String species_folder=args[0]; //ex: "Streptococcus_thermophilus_lmd_9.GCA_000014485.1.29.dna.chromosome.Chromosome.fa";
            String fasta_path=args[1];//ex:"Streptococcus_thermophilus_lmd_9.fa.txt";
            
            
            // search of palindrome building block 
           JavaRDD<String> kBlock4PalindromeArms=sc.textFile(stemLoopArmLen+"/"+species_folder);  
           JavaPairRDD<String,Integer>palindromeInput=mrsmrs.parseDevinOutput(kBlock4PalindromeArms);
           palindromeInput.saveAsTextFile("mrsmrs");
           JavaPairRDD <String, ArrayList<Integer>>  palindBlock=mrsmrs.ImperfectPalindromeAcrossGenomes(palindromeInput,stemLoopArmLen,loopLowBound,loopUpBound);
            palindBlock.saveAsTextFile("palindrome");
            //JavaPairRDD <String,ArrayList<Integer>> test_3=mrsmrs.extractPalinDromeArray(palindBlock,75,20,50,20,4); 
            //test_3.saveAsTextFile("crispr_test");
           //extension of palindrome building block
           
            //mri
            List<String> fasta_temp=sc.textFile(fasta_path).collect();
            String fasta="";
            for(int i=0; i< fasta_temp.size();i++){
                fasta=fasta+fasta_temp.get(i);
            }

             /////idas
            //List<String>fasta_temp=mrsmrs.readFile(fasta_path);
           // String fasta=fasta_temp.get(0);
           
            
            //JavaPairRDD<String,ArrayList<Integer>> test_4=mrsmrs.extendBuildingBlockArray(test_3,50, 20, 75, 20,fasta, 1,0,0,true,0.5);
            //test_4.saveAsTextFile("crispr_test2");
            JavaPairRDD<String, ArrayList<Integer>> test5=mrsmrs.extractTracrTrailCandidate( palindBlock,90, 15, 75,15,2,fasta,15, externalMaxStemLoopArmLen);
            test5.saveAsTextFile("crispr_test2");
            JavaPairRDD<String, ArrayList<Integer>> test6= mrsmrs.findMinimalTrailingArray(test5,palindromeInput,90 ,15 ,75,15,tracrAlignRatio,15);
            test6.saveAsTextFile("crispr_test3");

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
        public JavaPairRDD <String, ArrayList<Integer>> findMinimalTrailingArray(JavaPairRDD <String, ArrayList<Integer>> trailingCandidate , JavaPairRDD<String, Integer> arm_mer,int spacerMaxLen, int spacerMinLen, int unitMaxLen,int unitMinLen, double tracrAlignRatio, int lengthOfTrailingSeq) {
                final int r_max=unitMaxLen;
                final int r_min=unitMinLen;
                final int s_max=spacerMaxLen;
                final int s_min=spacerMinLen;
                final int unitDistMin=unitMinLen+spacerMinLen;
                final int unitDistMax=unitMaxLen+spacerMaxLen;
                final int trailingLen=lengthOfTrailingSeq;
                final double tracrAlignRatio2=tracrAlignRatio;
                
                JavaPairRDD<String,Iterable<Integer>> locations_per_repeat=arm_mer.groupByKey();
                JavaPairRDD<String,ArrayList<String>> selectedArmMer= locations_per_repeat.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>,String,ArrayList<String>>(){
                    @Override
                    public Iterable<Tuple2<String,ArrayList<String>>> call(Tuple2<String, Iterable<Integer>> keyValue){
                     Iterable<Integer>data =keyValue._2();
                     Iterator<Integer> itr=data.iterator();
                     String seq=keyValue._1();
                     ArrayList<Integer> locs_on_postiveStrand=new ArrayList<Integer>();
                     ArrayList<Tuple2<String, ArrayList<String>>> possibleRepeatUnits = new ArrayList<Tuple2<String, ArrayList<String>>> ();
    
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
                                       thisPositionSet.add(Integer.toString(thisPosLoc));
                                       thisPositionSet.add(Integer.toString(nextPosLoc));
                                       thisPositionSet.add(Integer.toString(nextTwoPosLoc));
                                       possibleRepeatUnits.add(new Tuple2<String,ArrayList<String>>(seq,thisPositionSet));
                                      
                                  }
                               
                              }
                                
                            }
                            
                                
                            }
                            
                      
                         }
                   
                     
    
                     return(possibleRepeatUnits);
    
                    }
    
                });
        
            final List<String> selectedArmSeq2= selectedArmMer.keys().collect();
            final int armlen=selectedArmSeq2.get(0).length();
            // use arm-mer with CRISPR-like architecture to select trailing seqeunce by matching
            // output : {matched_arm_seq, [trailingSeq_trailingLocation,matchedOrder]}
            JavaPairRDD<String ,ArrayList<String>> trailingSeq_matchedArmer = trailingCandidate.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<String>>(){
                    @Override
                    public Iterable<Tuple2<String,ArrayList<String>>> call(Tuple2<String, ArrayList<Integer>> keyValue){
                      ArrayList<Integer> tracrRelatedLocs =keyValue._2();
                      int trailingStart=tracrRelatedLocs.get(2);
                      String trailing_seq=keyValue._1();
                      String thisTrailingInfo=trailing_seq+"_"+trailingStart;
                      ArrayList<Tuple2<String, ArrayList<String>>> result1 = new ArrayList<Tuple2<String, ArrayList<String>>> ();
                      ArrayList<String> theseMatchedArms= new ArrayList<String>();

                      for(int i=0;i<trailingLen;i++){
                          if(i<trailingLen-armlen){
                            String thisWindowSeqTemp=trailing_seq.substring(i,i+armlen);
                            String thisWindowSeq="";// same strand
                           
                            for(int j=0;j<thisWindowSeqTemp.length();j++){
                                Character thisChar=thisWindowSeqTemp.charAt(j);
                                String  thisAlpha=thisChar.toString();
                                if(thisChar.equals("A")){
                                   thisWindowSeq=thisWindowSeq+"T";                                
                                }
                                
                                if(thisChar.equals("T")){
                                   thisWindowSeq=thisWindowSeq+"A";                                
                                }
                                
                                if(thisChar.equals("C")){
                                   thisWindowSeq=thisWindowSeq+"G";                                
                                }
                                
                                if(thisChar.equals("G")){
                                   thisWindowSeq=thisWindowSeq+"C";                                
                                }
                                
                            }
                           
                           
                            String thisWindowSeqRevCom=""; // reverse complimentary 
                            for(int j=thisWindowSeq.length()-1;j>=0;j--){
                                Character thisChar=thisWindowSeq.charAt(j);
                                thisWindowSeqRevCom=thisWindowSeqRevCom+thisChar.toString();
                                
                            }
                            
                            
                            
                            if(selectedArmSeq2.contains(thisWindowSeq)){
                                ArrayList<String> temp= new ArrayList<String>();
                                temp.add(thisTrailingInfo);
                                result1.add(new Tuple2<String, ArrayList<String>>(selectedArmSeq2.get(selectedArmSeq2.indexOf(thisWindowSeq)),temp));
                            }
                            
                            if(selectedArmSeq2.contains(thisWindowSeqRevCom)){
                                ArrayList<String> temp= new ArrayList<String>();
                                temp.add(thisTrailingInfo);
                                result1.add(new Tuple2<String, ArrayList<String>>(selectedArmSeq2.get(selectedArmSeq2.indexOf(thisWindowSeqRevCom)),temp));
                            }
                            
                          }
                      }

                      
                     return(result1);
                    }
                });


            JavaPairRDD <String,Tuple2<ArrayList<String>,ArrayList<String>>> mashup=trailingSeq_matchedArmer.join(selectedArmMer);

            // output format {seqOfTraing, [trailingstart, matchorder, arm_array_unit_starts]}
            JavaPairRDD <String, ArrayList<Integer>> mashup2= mashup.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<ArrayList<String>,ArrayList<String>>>,String,ArrayList<Integer>>(){
                     @Override
                     public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,Tuple2<ArrayList<String>,ArrayList<String>>> keyValue){
                         ArrayList<Tuple2<String, ArrayList<Integer>>> output2 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  
                         ArrayList<Integer>locs=new ArrayList<Integer>();
                         Tuple2<ArrayList<String>,ArrayList<String>> temp=keyValue._2();
                         
                         String[] temp2=temp._1().get(0).split("_");
                         int thisTrailingStart=Integer.parseInt(temp2[1]);
                         locs.add(thisTrailingStart);
                         String tracrSeq=temp2[0];
                         
                         for(int i=0;i<temp._2().size();i++){
                            locs.add(Integer.parseInt(temp._2().get(i)));
                         }
              
                        output2.add(new Tuple2<String, ArrayList<Integer>>(tracrSeq,locs));

                        return(output2); 
                     }
                });
            
            
            JavaPairRDD <String ,Iterable<ArrayList<Integer>>> mashup3=mashup2.groupByKey();
            JavaPairRDD <String, ArrayList<Integer>>result=mashup3.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<ArrayList<Integer>>>,String,ArrayList<Integer>>(){
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String,Iterable<ArrayList<Integer>>> keyValue){
                        String thisTrailingSeq=keyValue._1();
                        Iterable<ArrayList<Integer>> matchInfo =keyValue._2();
                        Iterator <ArrayList<Integer>> itr=matchInfo.iterator();
                        ArrayList<Tuple2<String, ArrayList<Integer>>>output3 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();
                        
                        String tracrStarts="";
                        ArrayList<Integer> firstUnitStarts =new ArrayList<Integer>();
                        ArrayList<Integer> secondUnitStarts =new ArrayList<Integer>();
                        ArrayList<Integer> thirdUnitStarts =new ArrayList<Integer>();
                  
                        while(itr.hasNext()){
                          ArrayList<Integer> thisMacthInfo=itr.next();  
                          
                          tracrStarts=tracrStarts+"_"+thisMacthInfo.get(0); // collecting  start locations of one trailing seq 
                          firstUnitStarts.add(thisMacthInfo.get(1));
                          secondUnitStarts.add(thisMacthInfo.get(2));
                          thirdUnitStarts.add(thisMacthInfo.get(3));
                             
                        }            
                        
                         ArrayList<Integer>firstUnitStartsSorted=firstUnitStarts;
                         Collections.sort(firstUnitStartsSorted);
                         ArrayList<Integer>filterdStarts=new ArrayList<Integer>();
                         // merging arm-mer which  can form a minimal array and reside in the same repeat unit
                         for(int i=0;i<firstUnitStartsSorted.size()-1;i++){
                              int thisFirstStart=firstUnitStartsSorted.get(i);
                              int nextFirstStart=firstUnitStartsSorted.get(i+1);
                              int interval=nextFirstStart-thisFirstStart;
                              if(interval>=armlen && interval<r_max-armlen){
                                  if(!filterdStarts.contains(thisFirstStart)){
                                      
                                      int  secondThisStart=secondUnitStarts.get(firstUnitStarts.indexOf(thisFirstStart));
                                      int  thirdThisStart=thirdUnitStarts.get(firstUnitStarts.indexOf(thisFirstStart));
                                      
                                      int  thisFirstDist=secondThisStart-thisFirstStart;
                                      int  thisSecondDist=thirdThisStart-secondThisStart;
                                      
                                      if(thisFirstDist>=r_min && thisSecondDist>=r_min){
                                          if(thisSecondDist<=r_max && thisFirstDist <=r_max){
                                             filterdStarts.add(thisFirstStart);
                                             filterdStarts.add(secondThisStart);
                                             filterdStarts.add(thirdThisStart);
                                              
                                          } 
                                      }
                    
                                        int  secondNextStart=secondUnitStarts.get(firstUnitStarts.indexOf(nextFirstStart));
                                        int  thirdNextStart=thirdUnitStarts.get(firstUnitStarts.indexOf(nextFirstStart));
                                        int  nextFirstDistance=secondNextStart-nextFirstStart;
                                        int  nextSecondDistance=thirdNextStart-secondNextStart;                           
                                        if(nextFirstDistance>=r_min && nextSecondDistance>=r_min){
                                            if(nextFirstDistance<=r_max && nextSecondDistance<=r_max){
                                                filterdStarts.add(nextFirstStart);
                                                filterdStarts.add(secondNextStart);
                                                filterdStarts.add(thirdNextStart);
                                            }
                                        }       
                                          
                                  }
                                
                                    

                                  
                              }
                         }
                      String outKey=thisTrailingSeq+tracrStarts;        
                      output3.add(new Tuple2<String, ArrayList<Integer>>(outKey,filterdStarts));     
                      return(output3);   
                    }
                        
            });     
            return(result);        
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
                         String leftSuspect=fasta.substring(thisPalinStar-numOfExtraBasesEachSide-1,thisPalinStar-2);
                         
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
                                //locations.add(thisPalinStar-i-lengthOfTrailingSeq-1);
                                //locations.add(thisPalinStar-i-lengthOfTrailingSeq+1);
                                //locations.add(thisPalinStar-i);
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

        
     
        
        /*purpose: record sequence/position during extension from every unit in the building block array
          mechanisms: extend first toward right, end and then left end due to 5'handle in the mechanism for crRNA processing
          output format: {buildingBlockSeq, [extended_unit1_start, extended_unit2_end,........,extended_unitN_start, extended_unitN_end]}
          buldingBlockSeq is seq of arm and length of loop   
        */
        public JavaPairRDD<String,ArrayList<Integer>>  extendBuildingBlockArray(JavaPairRDD<String,ArrayList<Integer>> buildingBlockArr,int maxRepLen, int minRepLen, int MaxSpace_rightrLen, int minSpacerLen, List<String>fasta, double support_ratio,double variance_right_ratio,double variance_left_ratio,boolean internal,double rightLenRatio){
            final double supportRatio=support_ratio;
            final double varianceRatio=variance_right_ratio;
            final double right_len_ratio=rightLenRatio;
            final List<String> fasta_seq=fasta;
            final int rep_max=maxRepLen ;
            final int rep_min=minRepLen;
            final int spa_min=minSpacerLen;
            final int spa_max=MaxSpace_rightrLen;
            final boolean is_internal=internal;
          
            JavaPairRDD <String,ArrayList<Integer>> result= buildingBlockArr.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>,String,ArrayList<Integer>>(){
                    
                    @Override
                    public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, ArrayList<Integer>> keyValue){
                    ArrayList<Tuple2<String, ArrayList<Integer>>> output2 = new ArrayList<Tuple2<String, ArrayList<Integer>>> ();  

                     // organizing information for subsequent processing
                     ArrayList<Integer> buildingBlockStarEndlocs =keyValue._2();
                     ArrayList<Integer> buildingBlockStarlocs= new ArrayList<Integer>();
                     
                     for(int i =0; i < buildingBlockStarEndlocs.size();i++){
                         if(i%2==0){
                             buildingBlockStarlocs.add(buildingBlockStarEndlocs.get(i)); 
                         }
                     }
                     
                     Collections.sort(buildingBlockStarlocs);
                     int buildingBlockCopies=buildingBlockStarlocs.size();
                     int [] finalStarts=new int[buildingBlockCopies];
                     int [] finalEnds=new int[buildingBlockCopies];
                     String[] buildingBlockSeqGapSize=keyValue._1().split(":");  //ex:AGTT:8
                     String gapSizeTemp=buildingBlockSeqGapSize[1];
                     int loopsize=Integer.parseInt(gapSizeTemp);
                     String armSeq=buildingBlockSeqGapSize[0];
                     int armLen=armSeq.length(); 
                     int avaiablespace=0;
                     int MaxSpace_right=0;
                     int MaxSpace_left=0;
                     
                     int palinSize=0;
                     if(is_internal){
                        
                        palinSize=2*armLen+loopsize;
                        avaiablespace=rep_max-palinSize;
                
                        
                     }
                     else{
                         int tracrStretch=armLen;
                         avaiablespace=rep_max-tracrStretch;
                     }
                     
                    MaxSpace_right=(int)Math.ceil(avaiablespace*right_len_ratio);
                    MaxSpace_left=avaiablespace-MaxSpace_right;
                    
                    final  int supportCopy=(int)Math.ceil(buildingBlockCopies*supportRatio);
                    final  int variantNum=(int)Math.floor(MaxSpace_right*varianceRatio);
                  
                  
                    

                    //geneation of potential extended regions
                     ArrayList<String> leftSeqs=new ArrayList<String>();
                     ArrayList<Integer>variantNumPerUnitCopy=new ArrayList<Integer>();
                     ArrayList<String> rightSeqs=new ArrayList<String>();
                     ArrayList<Integer> rightExtendStops=new ArrayList<Integer>();
                     ArrayList<Integer> leftExtendStarts=new ArrayList<Integer>();
                     for(int i=0;i<buildingBlockCopies;i++){
                          variantNumPerUnitCopy.add(0);
                          rightExtendStops.add(0);
                          leftExtendStarts.add(0);
                          finalStarts[i]=buildingBlockStarlocs.get(i);
                          if(is_internal){
                           finalEnds[i]=finalStarts[i]+palinSize-1;
                         
                          }
                          else{
                           finalEnds[i]=finalStarts[i]+armLen-1;
                          }
                          
                          int thisBlockStart=buildingBlockStarlocs.get(i);
                          int thisLeftEnd=thisBlockStart-1;
                          int thisLeftStart=thisLeftEnd-MaxSpace_left+1;
                          String thisLeftSeq=getSubstring(fasta_seq,thisLeftStart,thisLeftEnd);
                          leftSeqs.add(thisLeftSeq);
                          
           
                          
                          if(is_internal){
                            int thisBlockEnd=thisBlockStart+palinSize-1;
                            int thisRightStart=thisBlockEnd+1;
                            int thisRightEnd=thisRightStart+MaxSpace_right-1;
                            String thisRightSeq=getSubstring(fasta_seq,thisRightStart,thisRightEnd);
                            rightSeqs.add(thisRightSeq);
                           

                          }
                          
                          else{
                            int thisBlockEnd=thisBlockStart+armLen-1;
                            int thisRightStart=thisBlockEnd+1;
                            int thisRightEnd=thisRightStart+MaxSpace_right-1;
                            String thisRightSeq=getSubstring(fasta_seq,thisRightStart,thisRightEnd);
                            rightSeqs.add(thisRightSeq);
                        

                          }
                     }
                             
                      
                    int [] variantNumPerUnitCopyArr=new int[variantNumPerUnitCopy.size()];
                     for(int w=0;w<variantNumPerUnitCopy.size();w++){
                         variantNumPerUnitCopyArr[w]=variantNumPerUnitCopy.get(w);
                     }
    
                     //  to determine extension length toward right-hand side
                  
                     for(int j=0;j<MaxSpace_right;j++){
                         int[] baseCount={0,0,0,0}; // order : a,c,t,g
                         for(int k=0;k<rightSeqs.size();k++){
                             
                             String currentRightSeq=rightSeqs.get(k);
                      
                             char thisBase=currentRightSeq.charAt(j);
                             if(thisBase=='A'){
                                 baseCount[0]=baseCount[0]+1;
                             }
                             if(thisBase=='C'){
                                 baseCount[1]=baseCount[1]+1;
                             }
                             if(thisBase=='T'){
                                 baseCount[2]=baseCount[2]+1;
                             }
                             
                             if(thisBase=='G'){
                                 baseCount[3]=baseCount[3]+1;
                             }
                             
                       
                             
                         }
                             ArrayList<Integer> baseCountList=new ArrayList();
                             for(int t=0; t <baseCount.length;t++){
                                
                                 baseCountList.add(baseCount[t]);
                             } 
                             int maxBaseCount=Collections.max(baseCountList);
                             
             
                             
                             if(maxBaseCount>=supportCopy){
                                int maxIdx=baseCountList.indexOf(maxBaseCount);
                                char maxBase='q';
                                if(maxIdx==0){
                                    maxBase='A';
                                }
                                if(maxIdx==1){
                                    maxBase='C';
                                }
                                if(maxIdx==2){
                                    maxBase='T';
                                }
                                if(maxIdx==3){
                                    maxBase='G';
                                }
                             
                                for(int m=0;m<rightSeqs.size();m++){
                                  String targetRightSeq=rightSeqs.get(m);
                                  char targetBase=targetRightSeq.charAt(j);
                                  if(targetBase!=maxBase){
                                      variantNumPerUnitCopyArr[m]=variantNumPerUnitCopyArr[m]+1;
                                  }
                                }
                                
                           
                                
                                for(int b=0;b<variantNumPerUnitCopyArr.length;b++){
                                    int varNumThisUnit=variantNumPerUnitCopyArr[b];
                                    if(varNumThisUnit<=variantNum){
                                          //finalEnds[b]=finalEnds[b]+j+1;
                                          finalEnds[b]=finalEnds[b]+1; 
                                         
                                          int extension_step=j+1;
                                          
                                       
                                          
                                    }
                                    
                                    
                             
                                }
                            }
                         
                         
                            
                            else{ // consider this position has no consense base for every extened unit
                              for(int a=0;a<variantNumPerUnitCopy.size();a++){
                                  variantNumPerUnitCopyArr[a]=variantNumPerUnitCopyArr[a]+1;
                                  if(variantNumPerUnitCopyArr[a]<=variantNum){
                                          finalEnds[a]=finalEnds[a]+1;
                                        
                                      
                                    }
                              }
                            }
    
                     }
                    
                  
                    
                   //extension to left
                     variantNumPerUnitCopyArr=new int[variantNumPerUnitCopy.size()];
                     for(int w=0;w<variantNumPerUnitCopy.size();w++){
                         variantNumPerUnitCopyArr[w]=variantNumPerUnitCopy.get(w);
                     }
                   
                   
                   for(int j=MaxSpace_left-1;j>=0;j--){
                         int[] baseCount={0,0,0,0}; // order : a,c,t,g
                         for(int k=0;k<leftSeqs.size();k++){
                             
                             String currentLeftSeq=leftSeqs.get(k);
                      
                             char thisBase=currentLeftSeq.charAt(j);
                             if(thisBase=='A'){
                                 baseCount[0]=baseCount[0]+1;
                             }
                             if(thisBase=='C'){
                                 baseCount[1]=baseCount[1]+1;
                             }
                             if(thisBase=='T'){
                                 baseCount[2]=baseCount[2]+1;
                             }
                             
                             if(thisBase=='G'){
                                 baseCount[3]=baseCount[3]+1;
                             }
                             
                         }
                             ArrayList<Integer> baseCountList=new ArrayList();
                             for(int t=0; t <baseCount.length;t++){
                                
                                 baseCountList.add(baseCount[t]);
                             } 
                             int maxBaseCount=Collections.max(baseCountList);
                             
             
                             
                             if(maxBaseCount>=supportCopy){
                                int maxIdx=baseCountList.indexOf(maxBaseCount);
                                char maxBase='q';
                                if(maxIdx==0){
                                    maxBase='A';
                                }
                                if(maxIdx==1){
                                    maxBase='C';
                                }
                                if(maxIdx==2){
                                    maxBase='T';
                                }
                                if(maxIdx==3){
                                    maxBase='G';
                                }
                             
                                for(int m=0;m<leftSeqs.size();m++){
                                  String targetLeftSeq=leftSeqs.get(m);
                                  char targetBase=targetLeftSeq.charAt(j);
                                  if(targetBase!=maxBase){
                                      variantNumPerUnitCopyArr[m]=variantNumPerUnitCopyArr[m]+1;
                                  }
                                }
                                
                           
                                
                                for(int b=0;b<variantNumPerUnitCopyArr.length;b++){
                                    int varNumThisUnit=variantNumPerUnitCopyArr[b];
                                    if(varNumThisUnit<=variantNum){
                                          finalStarts[b]=finalStarts[b]-1; 
                                         
                                    }
                                    
                                    
                             
                                }
                            }
                         

                            else{ // consider this position has no consense base for every extened unit
                              for(int a=0;a<variantNumPerUnitCopy.size();a++){
                                  variantNumPerUnitCopyArr[a]=variantNumPerUnitCopyArr[a]+1;
                                  if(variantNumPerUnitCopyArr[a]<=variantNum){
                                          finalStarts[a]=finalStarts[a]-1;
                                        
                                      
                                    }
                              }
                            }
                            
   
                             
                     }
                   
                     // summarize two sets of extension for output
                     ArrayList<Integer> locations=new ArrayList<Integer>();
                     for(int r=0; r<finalStarts.length; r++){
                         int proposedRepeatUnitSize=finalEnds[r]-finalStarts[r]+1;
                         if(proposedRepeatUnitSize>=rep_min && proposedRepeatUnitSize<=rep_max){
                            locations.add(finalStarts[r]);
                            locations.add(finalEnds[r]);     
                            output2.add(new Tuple2<String, ArrayList<Integer>>(keyValue._1(),locations));     

                         }
                         
                     }            
                     return(output2);
                     }
                     
      
    
                    });
            
            return(result);
            
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
    
    

    
    
       // output: seq(left_arm) loc[gapSize,palindrome_start_i,palindrome_end_i]
        public  JavaPairRDD <String, ArrayList<Integer>>  extractPalinDromeArray( JavaPairRDD <String, ArrayList<Integer>> palindBlock, int spacerMaxLen, int spacerMinLen, int unitMaxLen,int unitMinLen,int arm_len){
                final int r_max=unitMaxLen;
                final int r_min=unitMinLen;
                final int s_max=spacerMaxLen;
                final int s_min=spacerMinLen;
                final int armLen=arm_len;
            
                JavaPairRDD<String,Iterable<ArrayList<Integer>>> palindBlockGroup=palindBlock.groupByKey();
            
            
            JavaPairRDD <String, ArrayList<Integer>> palindBlockArray= palindBlockGroup.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<ArrayList<Integer>>>,String,ArrayList<Integer>>(){
                @Override
                public Iterable<Tuple2<String,ArrayList<Integer>>> call(Tuple2<String, Iterable<ArrayList<Integer>>> keyValue){
                 Iterable<ArrayList<Integer>>start_loopsize =keyValue._2();
                 Iterator<ArrayList<Integer>> itr=start_loopsize.iterator();
                 int arm_len=keyValue._1().length();
                
                 ArrayList<Integer> palin_start=new ArrayList<Integer>();
                 ArrayList<Integer> palin_end=new ArrayList<Integer>();
                 List<Tuple2<String,ArrayList<Integer>>> result  =new ArrayList<Tuple2<String,ArrayList<Integer>>>();
                  
                 int palindromeSize=0; 
                 while(itr.hasNext()){
                   ArrayList<Integer> this_start_loopsize=itr.next();
                   int thisStart=this_start_loopsize.get(0);
                   int thisGapSize=this_start_loopsize.get(1);
                   int thisEnd=thisStart+thisGapSize+2*(arm_len-2)-1;
                   palindromeSize=thisEnd-thisStart+1;
                   palin_start.add(thisStart);
                   palin_end.add(thisEnd);
                 }

                Collections.sort(palin_start);
                Collections.sort(palin_end);
                int upperLimt=s_max+r_max-palindromeSize;
                for(int i=0;i<palin_start.size(); i++){
                    if(i<palin_start.size()-2){
                        int firstJuncDist=palin_start.get(i+1)-palin_start.get(i);
                        int secondJuncDist=palin_start.get(i+2)-palin_start.get(i+1);
                        int j=i+3;
                        if(firstJuncDist<=upperLimt && secondJuncDist<= upperLimt){
                            int startpos=palin_start.get(i);
                            int endpos=palin_end.get(i+2);
                            ArrayList<Integer>temp=new ArrayList<Integer>();
                            temp.add(palin_start.get(i));
                            temp.add(palin_end.get(i));
                            temp.add(palin_start.get(i+1));
                            temp.add(palin_end.get(i+1));
                            temp.add(palin_start.get(i+2));
                            temp.add(palin_end.get(i+2));
                            result.add(new Tuple2<String,ArrayList<Integer>>(keyValue._1(),temp));
                            //extend the array
                            while(j<palin_start.size()){
                                 int nextJunDis=palin_start.get(j)-palin_start.get(j-1);
                                 if(nextJunDis<upperLimt){
                                     endpos=palin_end.get(j);
                                     temp.add(palin_start.get(j));
                                     temp.add(palin_end.get(j));
                                     j=j+1;
                                 }
                                 else{
                                     result.add(new Tuple2<String,ArrayList<Integer>>(keyValue._1(),temp));
                                     j=palin_start.size();
                                 }
                                
                            }

                        }
                        
                        
                    } 
                }

                return(result);

            }

        });
        JavaPairRDD <String, ArrayList<Integer>> palinArray=palindBlockArray.distinct();
        return(palinArray);
            
           
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
    
