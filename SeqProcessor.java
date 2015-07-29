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

/*

CRISPR is an array of inverted repeats (approximately 20–50 bp each) separated by spacer sequences(approximately 20–75 bp each)[35].   
*/


public class SeqProcessor implements Serializable{

	public static void main(String [ ] args) throws Exception{
		SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
		JavaSparkContext sc=new JavaSparkContext(conf); 		
		SeqProcessor proc=new SeqProcessor();
		final double cutoff=0.70;

		JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Methanocaldococcus_jannaschii_dsm_2661.GCA_000091665.1.26.dna.chromosome.Chromosome.fa");
        
		JavaPairRDD<String,Long>  freq_region=proc.computeRegionFract(inputs,'A','T',cutoff);
        ArrayList<Long[]> possibleLeadRegion=proc.flagLeadLine(freq_region);


        JavaPairRDD<String,Long> threePrimeRegions= proc.flagThreePrimeLine( inputs,  possibleLeadRegion);
        JavaPairRDD<String,Integer> test= proc.getPossibleTransormedSpacerRegions( inputs,threePrimeRegions,20, 18);
        test.saveAsTextFile("crispr_test");
       // inputs.saveAsTextFile("crispr_test");

        // ArrayList<JavaPairRDD<String,Long>> result =proc.findCrisprRepeats( inputs,possibleLeadRegion,  threePrimeRegions);
        // JavaPairRDD<String,Long> test= result.get(0);
        // test.saveAsTextFile("crispr_test_2");
        // System.out.println("array list"+result.size());
        // for(int i=0; i<result.size();i++){
        //   System.out.println("rdd size:"+result.get(i).count());
        // }

           
    // String test="TTGATGGCCTGCTGTAAAAT";
  
    //    List<String> seqs=inputs.collect();
    //    String testsubstring=proc.getSubstring(seqs,24,43);
    //    System.out.println(testsubstring);
    //      String result=proc.rankBaseByDominance(testsubstring,18);
    // System.out.println(result);

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



    // use the first and last possible leader sequence to find potential lines that contain three prime flags
    public JavaPairRDD<String,Long> flagThreePrimeLine(JavaRDD<String> input, ArrayList<Long[]>  possibleLeadRegion ){
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
         // first filter then compute location
         JavaPairRDD<String,Long> potentialRegions=temp.filter(new Function<Tuple2<String, Long>, Boolean>(){
            @Override
            public Boolean call(Tuple2<String, Long> keyValue){
                Long rowNum=keyValue._2();
                String text=keyValue._1().toUpperCase();
                Boolean detected=true;
   
                if(possibleLeadRegion_transformed.contains(Integer.parseInt(rowNum.toString()))){
                


                    int firstThreePrimeLoc_1=text.indexOf("GAAAG");
                    int firstThreePrimeLoc_2=text.indexOf("GAAAC");
                    if(firstThreePrimeLoc_1>=0 ||firstThreePrimeLoc_2>=0){
                        detected= true;       
                    }

                    else{
                        detected=false;
                    }


                }

                else{
                    detected= false;
                }
                return(detected);
            }

        });

        return(potentialRegions);
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

        return(spacers);

    }


 // public ArrayList<JavaPairRDD<String,Long>> findCrisprRepeats(JavaRDD<String> fastaRdd,ArrayList<Long[]> possibleLeadRegion,JavaPairRDD<String,Long> threePrimeRegions){
 //            //determine whether the first three prime flag can be found within reasonable distance from specified leader sequence
 //            // assuming max size of repeat unit 150 bp
 //            int maxLineAway=3;
 //            final ArrayList<Long> nearestPossible3PrimeLines= new ArrayList<Long>();
 //            for(int i=0;i<possibleLeadRegion.size();i++){
 //               Long thisLeadEndLine=possibleLeadRegion.get(i)[1];
 //               Long thisThreePrimeEndLine=thisLeadEndLine+maxLineAway;

 //               for(int j=0;j<(thisThreePrimeEndLine-thisLeadEndLine);j++){
 //                nearestPossible3PrimeLines.add(thisLeadEndLine+1+j);
 //                }
 //            }

 //            JavaPairRDD<String,Long> firstRepeatUnitSeqLoc=threePrimeRegions.filter(new Function<Tuple2<String, Long>, Boolean>(){
 //                @Override
 //                public Boolean call(Tuple2<String, Long> keyValue){
 //                    Long thisLineNum=keyValue._2();
 //                    return nearestPossible3PrimeLines.contains(thisLineNum);
 //                }
 //            });

 //           // determine whether there are repeat happening
 //              //1. first compute distance between leader sequence and first flag to propose size of repeat unit , 
 //              //2. try to find repeat downstream allow shift for 2bp left or right
 //            JavaRDD<Integer> firstThreePrime=firstRepeatUnitSeqLoc.flatMap(new FlatMapFunction<Tuple2<String, Long>,Integer>(){
 //                @Override
 //                public Iterable<Integer> call(Tuple2<String, Long> keyValue){
 //                    ArrayList<Integer> threePrimeAbsLocs=new ArrayList<Integer>();
 //                    int thisLine=(int)(long)keyValue._2();
 //                    String thisText=keyValue._1();
 //                    boolean findMore=true;
 //                    int start_idx_1=0;
 //                    int start_idx_2=0;
 //                    while(findMore){
 //                        int idx_1=thisText.indexOf("GAAAG",start_idx_1);
 //                        int idx_2=thisText.indexOf("GAAAC",start_idx_2);
 //                        if(idx_1>=0){
 //                             threePrimeAbsLocs.add(60*(thisLine-1)+idx_1+1) ;                             
 //                             start_idx_1=idx_1+5;
 //                        }

 //                        else{
 //                            if(idx_2>=0){
 //                                threePrimeAbsLocs.add(60*(thisLine-1)+idx_2+1) ;                             
 //                                start_idx_2=idx_2+5;
 //                            }

 //                            else{
 //                                findMore=false;
 //                            }                               
 //                        }


 //                    }


 //                    return(threePrimeAbsLocs);
 //                }
 //            });

        
 //        List<Integer> allThreePrimeAbsStartLoc=firstThreePrime.collect();
 //        ArrayList<Integer> repeatPrimeAbsStartLoc=new ArrayList<Integer>();
 //        int max_repat_size=70; 

 //       final  List<String>fastaSeq=fastaRdd.collect();
 //        JavaRDD<Long> threePrimeFlagLines=threePrimeRegions.values();

 //        ArrayList<JavaPairRDD<String,Long>>result =new  ArrayList<JavaPairRDD<String,Long>>();

 //        for(int k=0;k<allThreePrimeAbsStartLoc.size();k++){
 //          int thisThreePrimeAbsStart=allThreePrimeAbsStartLoc.get(k);
 //          int potential_repeat_start=thisThreePrimeAbsStart-max_repat_size;
 //          int potential_repeat_end=thisThreePrimeAbsStart-1;
 //          if(potential_repeat_start<0||potential_repeat_end<0){
 //             break;
 //          }
 //          String potential_repeat_seq=getSubstring(fastaSeq,potential_repeat_start,potential_repeat_end);
          
 //          //test if inverted structure exisit
 //          final ArrayList<Integer> palindromeInProposedRepeatSeq=findPerfectPalindrome(potential_repeat_seq,4);
 //          final ArrayList<Integer> imperfectPalindromeInProposedRepeat=findImperfectPalindrome(potential_repeat_seq,3);
 //          if(palindromeInProposedRepeatSeq.size()==0  &&  imperfectPalindromeInProposedRepeat.size()<3){
 //            break;
 //          }

 //          final int lineWhereThisFlagIn=(int)Math.ceil(thisThreePrimeAbsStart/60)+1;
 //          final int thisThreePrimeAbsStartInLine=thisThreePrimeAbsStart-(lineWhereThisFlagIn-1)*60;


          
 //          JavaPairRDD<String,Long> possibleNextThreePrimeLine=threePrimeRegions.filter(new Function<Tuple2<String, Long>, Boolean>(){
 //            public Boolean call(Tuple2<String, Long> keyValue){
 //               int lineNum=Integer.parseInt(keyValue._2().toString());
 //               String  keyLine=keyValue._1();
 //               boolean palindromicHomology=false;
 //               boolean continueAnalysis=true;
 //               int max_repeat_size=70;
 //               int thisLineAbsStart=60*lineNum+1;
 //               int start_idx_1=0;
 //               int start_idx_2=0;
 //               while(continueAnalysis){
 //                  int idx_1=keyLine.indexOf("GAAAG",start_idx_1);
 //                  int idx_2=keyLine.indexOf("GAAAC",start_idx_2);

 //                  if(idx_1>=0){                                                           
 //                    int primeAbsStart=thisLineAbsStart+idx_1+1;             
 //                    int possibleNextRepeatStart=primeAbsStart-max_repeat_size;
 //                    int possibleNextRepeatEnd=primeAbsStart-1;
 //                    if(possibleNextRepeatStart<61 || possibleNextRepeatEnd<80){
 //                        break; 
 //                    }

 //                    else{
 //                      String possibleNextRepat=getSubstring(fastaSeq,possibleNextRepeatStart,possibleNextRepeatEnd);
 //                      ArrayList<Integer> thisPalindromStart=findPerfectPalindrome(possibleNextRepat,4);
 //                      ArrayList<Integer> thisImperfectPalindromStart=findImperfectPalindrome(possibleNextRepat,3);

 //                      if(thisImperfectPalindromStart.size()==palindromeInProposedRepeatSeq.size() && thisImperfectPalindromStart.size()==imperfectPalindromeInProposedRepeat.size()){
 //                        palindromicHomology=true;
 //                    }
 //                    start_idx_1=idx_1+5;              
 //                }
          

 //                    }
 //                  else{
 //                        if(idx_2>=0){
 //                        int primeAbsStart=thisLineAbsStart+idx_2-1;             
 //                        int possibleNextRepeatStart=primeAbsStart-max_repeat_size;
 //                        int possibleNextRepeatEnd=primeAbsStart-1;
 //                            if(possibleNextRepeatStart<61 || possibleNextRepeatEnd<80){
 //                            break; 
 //                            }
 //                            else{
 //                                String possibleNextRepat=getSubstring(fastaSeq,possibleNextRepeatStart,possibleNextRepeatEnd);
 //                                ArrayList<Integer> thisPalindromStart=findPerfectPalindrome(possibleNextRepat,4);
 //                                ArrayList<Integer> thisImperfectPalindromStart=findImperfectPalindrome(possibleNextRepat,3);

 //                                if(thisImperfectPalindromStart.size()==palindromeInProposedRepeatSeq.size() && thisImperfectPalindromStart.size()==imperfectPalindromeInProposedRepeat.size()){
 //                                    palindromicHomology=true;
 //                                }                           
 //                                start_idx_2=idx_2+5;
 //                            }    
 //                        }
                        

 //                        else{
 //                            continueAnalysis=false;
 //                        }                               
 //                    }
 //               }

 //               return(lineNum>=lineWhereThisFlagIn && palindromicHomology);
 //            }
 //          });


 //          if(possibleNextThreePrimeLine.count>0){
            
 //          }

 //          else{
 //            break;
 //          }
        


 //        }
 //        return(result);
        
 //    }


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
            System.out.println("start line:"+startLine);
           System.out.println("end line:"+endLine);
           System.out.println("start Idx:"+startLocIdxInLine);
           System.out.println("end Idx:"+endLocIdxInLine);
           // result=part.substring(startLocIdxInLine,part.length()-1)+part.charAt(endLocIdxInLine); 
            result=part.substring(startLocIdxInLine,endLocIdxInLine+1); 
           }
        else{
            System.out.println("start line:"+startLine);
           System.out.println("end line:"+endLine);
           System.out.println("start Idx:"+startLocIdxInLine);
           System.out.println("end Idx:"+endLocIdxInLine);
         result=part.substring(startLocIdxInLine,endLocIdxInLine+1);   
        }  
         
     }
     else{
        String middlePart="";
         if(endLine-startLine>1){
           System.out.println("start line:"+startLine);
           System.out.println("end line:"+endLine);
           System.out.println("start Idx:"+startLocIdxInLine);
           System.out.println("end Idx:"+endLocIdxInLine);
           
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
                 System.out.println("start line:"+startLine);
           System.out.println("end line:"+endLine);
           System.out.println("start Idx:"+startLocIdxInLine);
           System.out.println("end Idx:"+endLocIdxInLine);
           System.out.println("resul 1:"+part.substring(startLocIdxInLine,part.length()-1));
                      System.out.println("resul 2:"+seqFile.get(endLine).substring(0,endLocIdxInLine));

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
              if(!proposePalin.equals("ATCG")){

                result.add(i);
              }
            }
          }
        }


        return(result);
    } 

 // output: start idx of middle spacer
 // intervalLength minimum 3
 // arm-length requriment : 3 
    public ArrayList<Integer> findImperfectPalindrome (String seq, int intervalength){
        seq=seq.toUpperCase();
        int []scoreVetor=new int[seq.length()]; 
        ArrayList<Integer> PalindromeStartIdx= new ArrayList<Integer>();
        for(int i=0;i<seq.length();i++){

          Character thisChar=seq.charAt(i);
          if(thisChar=='A'){
            scoreVetor[i]=1;
          }

          if(thisChar=='T'){
            scoreVetor[i]=-1;
          }

          if(thisChar=='G'){
            scoreVetor[i]=2;
          }          

           if(thisChar=='C'){
            scoreVetor[i]=-2;
          }

        } 


        for(int i=0;i<seq.length();i++){
          if(i>=intervalength-1 && i<seq.length()-4){
            int totalSum=0;
            int innerSum=0;
            for(int j=0;j<intervalength;j++){
               totalSum=totalSum+scoreVetor[i+j];
            
               if(j!=0 && j!=intervalength-1){
                 innerSum=innerSum+scoreVetor[i+j];
               }
            }

          
            if(innerSum!=totalSum){
               
               if(scoreVetor[i-2]+ scoreVetor[i+intervalength+1] ==0 &&scoreVetor[i-1]+scoreVetor[i+intervalength]==0){ //idx i is the start idx of the middle spacer
                  PalindromeStartIdx.add(i);
                
               }            
            }            
          }
        }

        



        
      return(PalindromeStartIdx);
        
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
           System.out.println(A_count);
           System.out.println(C_count);
           System.out.println(G_count);
           System.out.println(T_count);
           for(int m=3;m>=0;m--){
               int alphaIndx=search.indexOf(refArry[m]);
               System.out.println("ref"+refArry[m]);
               System.out.println("alphaIndx"+alphaIndx);
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



