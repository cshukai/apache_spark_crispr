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
            String out_dir= "/idas/sc724/classI_result";
            /*user input*/
            
            //array structure
            int repeat_unit_min=15;
            int repeat_unit_max=70;
            int spacerMax=15;
            int spacerMin=90;
            
            //stem loop assocaited structures
            int stemLoopArmLen=4; // this is the minimal arm length of imperfect palindromes for internal stem loop
            int loopLowBound=3;
            int loopUpBound=8;

            
            
            /*processing*/
            // input directories to gneerate buildinb block
           String species_folder=args[0]; //ex: "Streptococcus_thermophilus_lmd_9.GCA_000014485.1.29.dna.chromosome.Chromosome.fa";
           String fasta_path=home_dir+"/singleLineGenome_genome/"+args[1];//ex:"Streptococcus_thermophilus_lmd_9.fa.txt";
           // String fasta_path=args[1];             
            
            // search of palindrome building block 
           JavaRDD<String> kBlock4PalindromeArms=sc.textFile(home_dir+"/kmer/"+stemLoopArmLen+"/"+species_folder);  

           JavaPairRDD<String,Integer>palindromeInput=mrsmrs.parseDevinOutput(kBlock4PalindromeArms);
          // JavaPairRDD<String,Integer>repeatUnitMers=mrsmrs.parseDevinOutput(buildingblock);
           
           palindromeInput.saveAsTextFile(out_dir+"/"+species_folder+"/mrsmrs");
           JavaPairRDD <String, ArrayList<Integer>>  palindBlock=mrsmrs.ImperfectPalindromeAcrossGenomes(palindromeInput,stemLoopArmLen,loopLowBound,loopUpBound);
           palindBlock.saveAsTextFile(out_dir+"/"+species_folder+"/palindrome");
            
    /////////////       
            //mri
            List<String> fasta_temp=sc.textFile(fasta_path).collect();
            String fasta="";
            for(int i=0; i< fasta_temp.size();i++){
                fasta=fasta+fasta_temp.get(i);
            }

           
            /*internal palindromes */ 
            JavaPairRDD <String,ArrayList<Integer>> test_3=mrsmrs.extractPalinDromeArray(palindBlock,50,20,50,20,4); 
            test_3.saveAsTextFile(out_dir+"/"+species_folder+"/crispr_test");
           //extension of palindrome building block
            JavaPairRDD<String,ArrayList<Integer>> test_4=mrsmrs.extendBuildingBlockArray(test_3,repeat_unit_max, repeat_unit_min, spacerMax, spacerMin,fasta_temp, 0.75,0.5,0.5,0.5);
            test_4.saveAsTextFile(out_dir+"/"+species_folder+"/crispr_test2");
         
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
        

        
        /*purpose: record sequence/position during extension from every unit in the building block array
          mechanisms: extend first toward right end and then left end due to 5'handle in the mechanism for crRNA processing
          output format: {buildingBlockSeq, [extended_unit1_start, extended_unit2_end,........,extended_unitN_start, extended_unitN_end]}
          input format :
          (1)buldingBlockSeq is seq of arm and length of loop
          (2) 
          
        */
        public JavaPairRDD<String,ArrayList<Integer>>  extendBuildingBlockArray(JavaPairRDD<String,ArrayList<Integer>> buildingBlockArr,int maxRepLen, int minRepLen, int MaxSpace_rightrLen, int minSpacerLen, List<String>fasta, double support_ratio,double variance_right_ratio,double variance_left_ratio,double rightLenRatio){
            final double supportRatio=support_ratio;
            final double varianceRatio=variance_right_ratio;
            final double right_len_ratio=rightLenRatio;
            final List<String> fasta_seq=fasta;
            final int rep_max=maxRepLen ;
            final int rep_min=minRepLen;
            final int spa_min=minSpacerLen;
            final int spa_max=MaxSpace_rightrLen;

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
                   
                    // calculate how maximum possible steps for  extension to left and rigth
                    palinSize=2*armLen+loopsize;
                    avaiablespace=rep_max-palinSize;
                    MaxSpace_right=(int)Math.ceil(avaiablespace*right_len_ratio);
                    MaxSpace_left=avaiablespace-MaxSpace_right;
                    //calcuate the cut-off value for determination of seqeunce variacne
                    final  int supportCopy=(int)Math.ceil(buildingBlockCopies*supportRatio);
                    final  int variantNum=(int)Math.floor(MaxSpace_right*varianceRatio); 
                    // sequence on the left of stem-loop should be more conserverd 
                    // so flexibility on right can gurantee sufficeint  tolerance on left

                    /*geneation of potential extended regions*/
                     ArrayList<String> leftSeqs=new ArrayList<String>();
                     ArrayList<Integer>variantNumPerUnitCopy=new ArrayList<Integer>();
                     ArrayList<String> rightSeqs=new ArrayList<String>();
                     ArrayList<Integer> rightExtendStops=new ArrayList<Integer>();
                     ArrayList<Integer> leftExtendStarts=new ArrayList<Integer>();
                     for(int i=0;i<buildingBlockCopies;i++){
                          variantNumPerUnitCopy.add(0);
                          rightExtendStops.add(0);
                          leftExtendStarts.add(0);
                          finalStarts[i]=buildingBlockStarlocs.get(i); // original positions before extension
                          finalEnds[i]=finalStarts[i]+palinSize-1;
    
                          int thisBlockStart=buildingBlockStarlocs.get(i);
                          int thisLeftEnd=thisBlockStart-1;
                          int thisLeftStart=thisLeftEnd-MaxSpace_left+1;
                          String thisLeftSeq=fasta_seq.get(0).substring(thisLeftStart,thisLeftEnd);
                          leftSeqs.add(thisLeftSeq); // longest possible left seq
                          
   
                          

                            int thisBlockEnd=thisBlockStart+palinSize-1;
                            int thisRightStart=thisBlockEnd+1;
                            int thisRightEnd=thisRightStart+MaxSpace_right-1;
                            String thisRightSeq=fasta_seq.get(0).substring(thisRightStart,thisRightEnd);
                            rightSeqs.add(thisRightSeq); //longest possible right sequence

            
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
                             if(j<currentRightSeq.length()){
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
                             else{
                                 break;
                             }
                             
                             
                       
                             
                         }
                         
                         
                             // determine if there is a nucleotide dominant sufficiently                 
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
                             
                                // keeping record of variance frequency
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
                            if(j<currentLeftSeq.length()){
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
                            
                            else{
                              break;   
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
    
