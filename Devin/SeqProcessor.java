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



public class SeqProcessor implements Serializable{

  public static void main(String [ ] args) throws Exception{
    SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
    JavaSparkContext sc=new JavaSparkContext(conf);     
    SeqProcessor proc=new SeqProcessor();
    final double cutoff=0.6;

    // JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Methanocaldococcus_jannaschii_dsm_2661.GCA_000091665.1.26.dna.chromosome.Chromosome.fa");
  //   JavaRDD<String> inputs_2=sc.textFile("bacteria/crispr/data/Methanobrevibacter_smithii_atcc_35061.GCA_000016525.1.26.dna.chromosome.Chromosome.fa");
    // JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Escherichia_coli_bl21_de3_gca_000009565_2.GCA_000009565.2.26.dna.genome.fa");
    // JavaRDD<String> inputs_2=sc.textFile("bacteria/crispr/data/Escherichia_coli_bl21_de3_gca_000022665_2.GCA_000022665.2.26.dna.genome.fa");

    // JavaPairRDD<String,Long>  freq_region=proc.computeRegionFract(inputs,'A','T',cutoff);
    // JavaPairRDD<String,Long>  freq_region_2=proc.computeRegionFract(inputs_2,'A','T',cutoff);

    // ArrayList<Long[]> possibleLeadRegion=proc.flagLeadLine(freq_region);
    // ArrayList<Long[]> possibleLeadRegion_2=proc.flagLeadLine(freq_region_2);

      
    //   JavaPairRDD<String,Iterable<Integer>> test= proc.flagPossibleRepeat(inputs,possibleLeadRegion);

    //     List<Iterable<Integer>> result= new ArrayList<Iterable<Integer>>();
    //     result=test.lookup("TCGATCGATCGA");
    //     Iterable<Integer> data=result.get(0);
    //     Iterator<Integer> itr=data.iterator();

    //     while(itr.hasNext()){
    //         System.out.println(itr.next());
    //     }
       

  
        
        // JavaPairRDD<String,Integer> test2=test.groupByKey();
        // test2.saveAsTextFile("crispr_test_2");
       // inputs.saveAsTextFile("crispr_test");

        // ArrayList<JavaPairRDD<String,Long>> result =proc.findCrisprRepeats( inputs,possibleLeadRegion,  threePrimeRegions);
        // JavaPairRDD<String,Long> test= result.get(0);
        // test.saveAsTextFile("crispr_test_2");
        // System.out.println("array list"+result.size());
        // for(int i=0; i<result.size();i++){
        //   System.out.println("rdd size:"+result.get(i).count());
        // }

           
  
    //    List<String> seqs=inputs.collect();
    //    String testsubstring="AGGCCGGGTTTGCTTTTATG";
    //      String result=proc.rankBaseByDominance(testsubstring,18);
    // System.out.println(result);


      String test="CCATTACCCCCATAT";
      ArrayList<Integer> result = proc.findPerfectPalindrome(test,4);
      for(int i=0;i<result.size();i++){
        System.out.println(result.get(i));
      }

  }





    //nu_1,nu_2 are upper case 
    //0-poor 1-rich
    //01,1  second line has right region enriched
  
  public JavaPairRDD<String,Long> computeRegionFract(JavaRDD<String>  seqFiles, Character base_1,Character base_2, double cutoff ){
        final double cut=cutoff;
        final Character nu_1=base_1;
        final Character nu_2=base_2;
        JavaRDD<String> di_rich_regions=seqFiles.map(new Function<String,String> (){
          @Override
          public String call(String line) {
                                           
                line=line.toUpperCase();
                
                double left_hit=0.00;
                double right_hit=0.00;
                double seq_length=line.length();
                String result="";
                for(int i=0;i<seq_length;i++){
                  
                  Character thisLetter=line.charAt(i);     
                  if(thisLetter.equals(nu_1)||thisLetter.equals(nu_2)){
                    if(i>(seq_length/2)){
                      right_hit=right_hit+1;
                    }
                    else{
                      left_hit=left_hit+1;
                    }
                  }
                
                }

                double left_fraction=left_hit/(seq_length/2);
                double right_fraction=right_hit/(seq_length/2);

                if(left_fraction<cut && right_fraction<cut) {
                    result="00";
                }

                 if(left_fraction>cut && right_fraction<cut) {
                    result="10";
                }

                if(left_fraction>cut && right_fraction>cut) {
                    result="11";
                }


                if(left_fraction<cut && right_fraction>cut) {
                    result="01";
                }                  

            return result;
          }
        }); 

      return (di_rich_regions.zipWithIndex());


  }
   
  // convert to 3 mers and 5 mers as building blocks for imperfect palindrome, 4 mers for perfect palindrome
//   public JavaPairRDD<String,Integer> fastaRDD2kmers(JavaRDD<String> fasta){
//     final JavaPairRDD<String,Long> fastaRDD=fasta.zipWithIndex();
//     final List<String> fastaText=fasta.collect();
//     JavaPairRDD<String,Integer> result=fastaRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Long>,String,Integer>(){
//       @Override
//       public Iterable<Tuple2<String,Integer>> call(Tuple2<String, Long> keyValue){

//         if(lineNum>0){
//          ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>> ();
//          int lineNum=Integer.parseInt(keyValue._2().toString());
//          String seq=keyValue._1().toUpperCase();  

//          int[] expand=new int[seq.length()+1];
//          expand[0]=0;
//          int[] cumulative=new int[seq.length()];
//          int[] transVec=new int[seq.length()];

//          for(int i=0;i<seq.length();i++){
//           int transformedValue=0;
//           Character thisLetter=seq.charAt(i);
//           if(thisLetter.equals('A')){
//             transformedValue=1;
//           }
//           if(thisLetter.equals('T')){
//             transformedValue=-1;
//           }
//           if(thisLetter.equals('C')){
//             transformedValue=7;
//           }
//           if(thisLetter.equals('G')){
//             transformedValue=-7;
//           }

//           if(i==0){
//             cumulative[i]=transformedValue;
//             expand[i+1]=transformedValue;

//           }
//           else{
//             cumulative[i]=cumulative[i-1]+transformedValue;
//             expand[i+1]=cumulative[i];
//           } 

//         }

//         // start to find perfect palindrome
//         boolean potential=false;
//         int[] substractVec=new int[seq.length()-palindromeLen+1];
//         int start=palindromeLen-1;
//         for(int i=start;i<seq.length();i++){
//           substractVec[i-(palindromeLen-1)]=cumulative[i]-expand[i-(palindromeLen-1)];
//           if(substractVec[i-(palindromeLen-1)]==0){
//            potential=true;
//          }
//        }

//        ArrayList<Integer> palinStarts= new ArrayList<Integer>();
//         if(potential){
//           for(int i=0;i<substractVec.length;i++){
//             if(substractVec[i]==0){
//               String proposePalin=seq.substring(i,i+3);
//               if(!proposePalin.equals("ATCG")){

//                 palinStarts.add(i);
//               }
//             }
//           }
//         }
         

//           for(int i=0; i<palinStarts.size();i++){
//                int thisPalinStar=palinStarts.get(i)+(lineNum-1)*60;
//                 result.add(new Tuple2<String,Integer>(getSubstring(fastaText,thisPalinStar,thisPalinStar+3),thisPalinStar));
//           } 


//           //start to find imperfect palindrome



                    

//      }


//      return(result);   
//    }
//  });

// }




    // output :[start_line_num,end_line_number]
    public  ArrayList<Long[]> flagLeadLine(JavaPairRDD<String,Long> freq_region){
        List<Long> full_regions=freq_region.lookup("11");
        List<Long> left_rich_regions=freq_region.lookup("10");
        List<Long> right_rich_regions=freq_region.lookup("01");

      //extend full regions 
        ArrayList<Long[]> result= new ArrayList<Long[]>();  
        
        for(int i=0; i<full_regions.size();i++){
            Long this_full_line=full_regions.get(i);

            int left_rich_idx=left_rich_regions.indexOf(this_full_line-1);
            int right_rich_idx=right_rich_regions.indexOf(this_full_line+1);
            Long[] this_result=new Long[2];
            if(left_rich_idx>=0){
               this_result[0]=left_rich_regions.get(left_rich_idx);
            }

            else{
               this_result[0]=this_full_line;    
            }

            if(right_rich_idx>=0){
               this_result[1]=right_rich_regions.get(right_rich_idx);
            }
            else{
               this_result[1]=this_full_line;    
            }

            result.add(this_result);
        }

      return(result);
    }



    // use the first and last possible leader sequence to find potential repeat and spacer locus
    // use rank-based seqeunce to separate repeat and spacer seqeunce

    public JavaPairRDD<String,Iterable<Integer>> flagPossibleRepeat(JavaRDD<String> input, ArrayList<Long[]>  possibleLeadRegion ){
         final JavaPairRDD<String,Long> temp=input.zipWithIndex();
        
         //generate possibe regeions based on locaiotn of leader sequences
        final ArrayList<Integer> possibleLeadRegion_transformed=new ArrayList<Integer>();
         for (int i=0;i<possibleLeadRegion.size();i++){
              if(i==possibleLeadRegion.size()-1){
                   int thisStart=Integer.parseInt(possibleLeadRegion.get(i)[1].toString());
                   int thisEnd=(int)(temp.count()-1);
                   for(int j=thisStart;j<thisEnd;j++){
                    possibleLeadRegion_transformed.add(j);
                   }
                   
              }    

              else{
                   int thisStart=Integer.parseInt(possibleLeadRegion.get(i)[1].toString());;
                   int thisEnd=Integer.parseInt(possibleLeadRegion.get(i+1)[0].toString());;
                   for(int j=thisStart;j<thisEnd;j++){
                    possibleLeadRegion_transformed.add(j);
                   }
              }
         }

         JavaPairRDD<String,Long> potentialRegions=temp.filter(new Function<Tuple2<String, Long>, Boolean>(){
            @Override
            public Boolean call(Tuple2<String, Long> keyValue){
                Long rowNum=keyValue._2();
                String text=keyValue._1().toUpperCase();
                return(possibleLeadRegion_transformed.contains(Integer.parseInt(rowNum.toString())));
            }

        });

         // for every element in potetinalRegions, rank seq basesd on 20 mers  and calculate location  
         // return a JavaPairRDD to record ranked-seq and start location of that rank-seq
          JavaPairRDD<String,Integer> rankSeqOfRepeatSpacer=potentialRegions.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Long>,String,Integer>(){
                @Override
                public Iterable<Tuple2<String,Integer>> call(Tuple2<String, Long> keyValue){
                    int lineNum=Integer.parseInt(keyValue._2().toString());
                    String text=keyValue._1().toUpperCase();  
                    String front=text.substring(0,19);
                    String middle=text.substring(20,39);
                    String back=text.substring(40,59); 

                    String front_ranked=rankBaseByDominance(front,17); 
                    String middle_ranked=rankBaseByDominance(middle,17); 
                    String back_ranked=rankBaseByDominance(back,17); 

                    int front_start=lineNum*60+1;
                    int middle_start=lineNum*60+21;
                    int back_start=lineNum*60+41;

                    ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>> ();
                    result.add(new Tuple2<String,Integer>(front_ranked,front_start));
                    result.add(new Tuple2<String,Integer>(middle_ranked,middle_start));
                    result.add(new Tuple2<String,Integer>(back_ranked,back_start));



                    return(result);   
                }
           });


        JavaPairRDD<String,Iterable<Integer>> result=rankSeqOfRepeatSpacer.groupByKey();
        final List<String> fastaseqs=input.collect();
        
        


        return(result);
    }


    public JavaPairRDD<String,Integer> getPossibleTransormedSpacerRegions(JavaRDD<String> seqs,JavaPairRDD<String,Long> threePrimeLine,int spacer_size,int windowSize){
           final int spacer_seg_len=spacer_size;
           final List<String> fastaSeqs=seqs.collect();
           final int rankWindowSize=windowSize;
           JavaPairRDD<String,Integer> spacers=threePrimeLine.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Long>,String,Integer>(){
                @Override
                public Iterable<Tuple2<String,Integer>> call(Tuple2<String, Long> keyValue){
                    Long lineNum=keyValue._2();
                    String text=keyValue._1().toUpperCase();
                    int start_idx_1=0;
                    int start_idx_2=0;
                    boolean findMore=true;
                    ArrayList<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>> ();
                    while(findMore){
                        int idx_1=text.indexOf("GAAAG",start_idx_1);
                        int idx_2=text.indexOf("GAAAC",start_idx_2);
                        if(idx_1>=0){
                             int thisThreePrimeAbsStart=(Integer.parseInt(lineNum.toString())-1)*60+idx_1+1;
                             int thisThreePrimeAbsEnd=thisThreePrimeAbsStart+spacer_seg_len-1;
                             String thisSeq=getSubstring(fastaSeqs,thisThreePrimeAbsStart,thisThreePrimeAbsEnd);
                             String thisRank=rankBaseByDominance(thisSeq,rankWindowSize);
                             result.add(new Tuple2<String,Integer>(thisRank,thisThreePrimeAbsStart));                           
                             start_idx_1=idx_1+5;
                        }

                        else{
                            if(idx_2>=0){
                                 int thisThreePrimeAbsStart=(Integer.parseInt(lineNum.toString())-1)*60+idx_2+1;
                                 int thisThreePrimeAbsEnd=thisThreePrimeAbsStart+spacer_seg_len-1;
                                 String thisSeq=getSubstring(fastaSeqs,thisThreePrimeAbsStart,thisThreePrimeAbsEnd);
                                 String thisRank=rankBaseByDominance(thisSeq,rankWindowSize);
                                 result.add(new Tuple2<String,Integer>(thisRank,thisThreePrimeAbsStart));                           
                                 start_idx_2=idx_2+5;
                            }

                            else{
                                findMore=false;
                            }                               
                        }
                    } 
                    return(result);   
                }
           });
        
        JavaRDD<Integer> temp=spacers.values();
        List<Integer>spacersStarts=temp.takeOrdered((int)temp.count());
       final  ArrayList<Integer> spacersStarts_refined=new ArrayList<Integer>();
        for(int i=0; i<spacersStarts.size();i++){
            int thisStart=spacersStarts.get(i);
            
            if(i==spacersStarts.size()-1){
                  break;
            }
              if(spacersStarts.get(i+1)-thisStart<=200){//distance between two adjacent spacer should not be over 200
                spacersStarts_refined.add(thisStart);
            }  

        }

        JavaPairRDD<String,Integer> spacers_filtered=spacers.filter(new Function<Tuple2<String,Integer>, Boolean>(){
            public Boolean call(Tuple2<String,Integer> keyValue){
                int thisStart=keyValue._2();
                return(spacersStarts_refined.contains(thisStart));
            }
        });
        
        //JavaPairRDD<String,Iterable<Integer>> result=spacers_filtered.groupByKey();
        return(spacers_filtered);

    }

    // output array : 0- spacer start for spec 1 ;1-spacer start for spec 2
    //ArrayList<Integer>[]
    public void  getSpacerStarts(JavaRDD<String> seqs,JavaRDD<String> seqs_2,JavaPairRDD<String,Integer> spec_1_poss_spacer,JavaPairRDD<String,Integer> spec_2_poss_spacer){
        ArrayList<Integer> spec1_result=new ArrayList<Integer>();
        ArrayList<Integer> spec2_result=new ArrayList<Integer>();
        
        List<String> seqs_string=seqs.collect();
        List<String> seqs_string_2=seqs_2.collect();
        JavaRDD<String> spec_1_key=spec_1_poss_spacer.keys();
        JavaRDD<String> spec_2_key=spec_2_poss_spacer.keys();
        List<String> common_keys=spec_1_key.intersection(spec_2_key).collect();
         
         
         for(int i=0;i<common_keys.size();i++){
          String thisCommokey=common_keys.get(i);
          final List<Integer> pos_spec1=spec_1_poss_spacer.lookup(thisCommokey);
          final List<Integer> pos_spec2=spec_2_poss_spacer.lookup(thisCommokey);
          
          // filtering out impossible left position 
          JavaPairRDD<String,Integer> this_left_pos_spec1= spec_1_poss_spacer.filter(new Function<Tuple2<String, Integer>, Boolean>(){
                @Override
                public Boolean call(Tuple2<String,Integer> keyValue){
                    boolean detected=false;
                    int thisStart=keyValue._2();
                    for(int j=0;j<pos_spec1.size();j++){
                      if(pos_spec1.get(j)-thisStart<200 && pos_spec1.get(j)-thisStart>=20){
                        detected=true;
                      }
                    }       
                    return (detected);
                }
            });

          JavaPairRDD<String,Integer> this_left_pos_spec2= spec_2_poss_spacer.filter(new Function<Tuple2<String, Integer>, Boolean>(){
                @Override
                public Boolean call(Tuple2<String,Integer> keyValue){
                    boolean detected=false;
                    int thisStart=keyValue._2();
                    for(int j=0;j<pos_spec2.size();j++){
                      if(pos_spec2.get(j)-thisStart<200 && pos_spec2.get(j)-thisStart>=20){
                        detected=true;
                      }
                    }       
                    return (detected);
                }
            });

          // find if spec 1 and spec 2 have same transition pattern in terms of rank seq
           
          List<String> commonRankSeqs=this_left_pos_spec1.keys().intersection(this_left_pos_spec2.keys()).collect();
          if(commonRankSeqs.size()==0){
   

             break;
             
          }

          else{
              for(int k=0;k<commonRankSeqs.size();k++){
              
                   String thisRankSeq=commonRankSeqs.get(k);
                   List<Integer> spec1_spacers=this_left_pos_spec1.lookup(thisRankSeq);
                   List<Integer> spec2_spacers=this_left_pos_spec2.lookup(thisRankSeq);

                   for(int m=0;m<spec1_spacers.size();m++){
                       int this_spec_1_left_pos=spec1_spacers.get(m);

                       for(int n=0;n<pos_spec1.size();n++){
                           int this_pos_spec_1=pos_spec1.get(n);
                           if(this_pos_spec_1-this_spec_1_left_pos<200 && this_pos_spec_1-this_spec_1_left_pos>=20 ){
                            String left_string_1=getSubstring(seqs_string,this_spec_1_left_pos,this_spec_1_left_pos+20);
                            String thisString_1=getSubstring(seqs_string,this_pos_spec_1,this_pos_spec_1+20);

                              for(int a=0;a<spec2_spacers.size();a++){
                                 int this_spec_2_left_pos=spec2_spacers.get(a);
                                 for(int q=0;q<pos_spec2.size();q++){

                                    int this_pos_spec_2=pos_spec2.get(q);
                                    if(this_pos_spec_2-this_spec_2_left_pos<200 && this_pos_spec_2-this_spec_2_left_pos>=20 ){
                                        String left_string_2=getSubstring(seqs_string_2,this_spec_2_left_pos,this_spec_2_left_pos+20);
                                        String thisString_2=getSubstring(seqs_string_2,this_pos_spec_2,this_pos_spec_2+20);
                                      
                                         int alignmentScore=0;
                                         int alingmentScore_left=0;
                                         
                                          
                                       for(int t=0;t<thisString_1.length();t++){

                                          if(thisString_1.charAt(t)==thisString_2.charAt(t)){
                                              alignmentScore=alignmentScore+1;
                                                                
                                          }
                                       }   
                                      
                                      for(int w=0;w<left_string_1.length();w++){
                                          if(left_string_1.charAt(w)==left_string_2.charAt(w)){
                                              alingmentScore_left=alingmentScore_left+1;
                                                                   
                                          }
                                      }

                                             
                                      
                                      if(alignmentScore>=18 &&alingmentScore_left>=18){
                                       spec1_result.add(this_spec_1_left_pos);
                                       spec1_result.add(this_pos_spec_1);
                                       spec2_result.add(this_spec_2_left_pos);
                                       spec2_result.add(this_pos_spec_2);
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
    }



    // output is 1-based start loc in input string
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


// }

return(result);

}




    //Ye, C., Ji, G., Li, L., & Liang, C. (2014). detectIR: A Novel Program for Detecting Perfect and Imperfect Inverted Repeats Using Complex Numbers and Vector Calculation. PLoS ONE, 9(11), e113349. doi:10.1371/journal.pone.0113349
    public ArrayList<Integer> findPerfectPalindrome(String seq,int palindromeLen){ // min legnth =4
        seq=seq.toUpperCase();

        int[] expand=new int[seq.length()+1];
        expand[0]=0;
        int[] cumulative=new int[seq.length()];
        int[] transVec=new int[seq.length()];

        for(int i=0;i<seq.length();i++){
          int transformedValue=0;
          Character thisLetter=seq.charAt(i);
          if(thisLetter.equals('A')){
            transformedValue=1;
          }
          if(thisLetter.equals('T')){
            transformedValue=-1;
          }
          if(thisLetter.equals('C')){
            transformedValue=7;
          }
          if(thisLetter.equals('G')){
            transformedValue=-7;
          }
          transVec[i]=transformedValue;

          if(i==0){
            cumulative[i]=transformedValue;
            expand[i+1]=transformedValue;

          }
          else{
            cumulative[i]=cumulative[i-1]+transformedValue;
            expand[i+1]=cumulative[i];
          } 

        }

        boolean potential=false;
        int[] substractVec=new int[seq.length()-palindromeLen+1];
        int start=palindromeLen-1;
        for(int i=start;i<seq.length();i++){
          substractVec[i-(palindromeLen-1)]=cumulative[i]-expand[i-(palindromeLen-1)];
          if(substractVec[i-(palindromeLen-1)]==0){
             potential=true;
          }
        }


        ArrayList<Integer> result= new ArrayList<Integer>();
        if(potential){
          for(int i=0;i<substractVec.length;i++){
            if(substractVec[i]==0){
              String proposePalin=seq.substring(i,i+3);
              if(transVec[i]+transVec[i+3]==0){

                result.add(i);
              }
            }
          }
        }


        return(result);
    } 



    // input : k- length of kmer
    public String  rankBaseByDominance (String seq,int k){
       seq=seq.toUpperCase();
       
       String result="";
       for(int i=0;i<seq.length();i++){
           if(i+k-1>=seq.length()){
            break;
           }
           else{
            String thisWindow=seq.substring(i,i+k);
            int A_count=0;
            int T_count=0;
            int C_count=0;
            int G_count=0;
            for(int j=0;j<thisWindow.length();j++){
                Character thisChar=thisWindow.charAt(j);
                if(thisChar.equals('A')){
                  A_count=A_count+1;
                }

                if(thisChar.equals('C')){
                  C_count=C_count+1;
                }
                 if(thisChar.equals('G')){
                  G_count=G_count+1;
                }
                 if(thisChar.equals('T')){
                  T_count=T_count+1;
                }



            }
           int[] refArry=new int[4];
           refArry[0]=A_count;
           refArry[1]=C_count;
           refArry[2]=G_count;
           refArry[3]=T_count;      
           Arrays.sort(refArry);

           ArrayList<Integer> search= new ArrayList<Integer> ();
           search.add(A_count);
           search.add(C_count);
           search.add(G_count);
           search.add(T_count);
       
           for(int m=3;m>=0;m--){
               int alphaIndx=search.indexOf(refArry[m]);
               
               if(alphaIndx==0){
                result=result+"A";
               }
                if(alphaIndx==1){
                result=result+"C";
               }
                if(alphaIndx==2){
                result=result+"G";
               }
               if(alphaIndx==3){
                result=result+"T";
               }
           }



           }

           
       }

       
       return(result);
       
    }


 }



