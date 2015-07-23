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

		//JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Methanocaldococcus_jannaschii_dsm_2661.GCA_000091665.1.26.dna.chromosome.Chromosome.fa");
    String test="CCCGCATGAAAACGT";
    ArrayList<Integer>result=proc.findPerfectPalindrome(test,4);
    System.out.println("total:"+result.size()+"first:"+result.get(0)+"second:"+result.get(1)+"thrid"+result.get(2));
        
		// JavaPairRDD<String,Long>  freq_region=proc.computeRegionFract(inputs,'A','T',cutoff);
  //       ArrayList<Long[]> possibleLeadRegion=proc.flagLeadSeq(freq_region);

  //       JavaPairRDD<String,Long> threePrimeRegions= proc.flagThreePrimeLoc( inputs,  possibleLeadRegion);
        // ArrayList<Integer> test= proc.findCrisprRepeats( possibleLeadRegion,  threePrimeRegions);
        // for(int i=0; i<test.size();i++){
        //   System.out.println(test.get(i));
        // }
        
    
        //threePrimeRegions.saveAsTextFile("crispr_test");

       

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
    public  ArrayList<Long[]> flagLeadSeq(JavaPairRDD<String,Long> freq_region){
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
    public JavaPairRDD<String,Long> flagThreePrimeLoc(JavaRDD<String> input, ArrayList<Long[]>  possibleLeadRegion ){
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



    //     public ArrayList<Integer> findCrisprRepeats(JavaRDD<String> fastaRdd,ArrayList<Long[]> possibleLeadRegion,JavaPairRDD<String,Long> threePrimeRegions){
    //         //determine whether the first three prime flag can be found within reasonable distance from specified leader sequence
    //         // assuming max size of repeat unit 150 bp
    //         int maxLineAway=3;
    //         final ArrayList<Long> nearestPossible3PrimeLines= new ArrayList<Long>();
    //         for(int i=0;i<possibleLeadRegion.size();i++){
    //            Long thisLeadEndLine=possibleLeadRegion.get(i)[1];
    //            Long thisThreePrimeEndLine=thisLeadEndLine+maxLineAway;

    //            for(int j=0;j<(thisThreePrimeEndLine-thisLeadEndLine);j++){
    //             nearestPossible3PrimeLines.add(thisLeadEndLine+1+j);
    //             }
    //         }

    //         JavaPairRDD<String,Long> firstRepeatUnitSeqLoc=threePrimeRegions.filter(new Function<Tuple2<String, Long>, Boolean>(){
    //             @Override
    //             public Boolean call(Tuple2<String, Long> keyValue){
    //                 Long thisLineNum=keyValue._2();
    //                 return nearestPossible3PrimeLines.contains(thisLineNum);
    //             }
    //         });

    //        // determine whether there are repeat happening
    //           //1. first compute distance between leader sequence and first flag to propose size of repeat unit , 
    //           //2. try to find repeat downstream allow shift for 2bp left or right
    //         JavaRDD<Integer> firstThreePrime=firstRepeatUnitSeqLoc.flatMap(new FlatMapFunction<Tuple2<String, Long>,Integer>(){
    //             @Override
    //             public Iterable<Integer> call(Tuple2<String, Long> keyValue){
    //                 ArrayList<Integer> threePrimeAbsLocs=new ArrayList<Integer>();
    //                 int thisLine=(int)(long)keyValue._2();
    //                 String thisText=keyValue._1();
    //                 boolean findMore=true;
    //                 int start_idx_1=0;
    //                 int start_idx_2=0;
    //                 while(findMore){
    //                     int idx_1=thisText.indexOf("GAAAG",start_idx_1);
    //                     int idx_2=thisText.indexOf("GAAAC",start_idx_2);
    //                     if(idx_1>=0){
    //                          threePrimeAbsLocs.add(60*(thisLine-1)+idx_1+1) ;                             
    //                          start_idx_1=idx_1+5;
    //                     }

    //                     else{
    //                         if(idx_2>=0){
    //                             threePrimeAbsLocs.add(60*(thisLine-1)+idx_2+1) ;                             
    //                             start_idx_2=idx_2+5;
    //                         }

    //                         else{
    //                             findMore=false;
    //                         }                               
    //                     }


    //                 }


    //                 return(threePrimeAbsLocs);
    //             }
    //         });


    //     List<Integer> allThreePrimeAbsStartLoc=firstThreePrime.collect();
    //     ArrayList<Integer> repeatPrimeAbsStartLoc=new ArrayList<Integer>();
    //     int max_repat_size=70;        
    //     List<String>fastaSeq=fastaRdd.collect();
    //     JavaRDD<Long> threePrimeFlagLines=threePrimeRegions.values();


    //     for(int k=0;k<allThreePrimeAbsStartLoc.size();k++){
    //       int thisThreePrimeAbsStart=allThreePrimeAbsStartLoc.get(i);
    //       int potential_repeat_start=thisThreePrimeAbsStart-max_repat_size;
    //       int potential_repeat_end=thisThreePrimeAbsStart-1;
    //       String potential_repeat_seq=getSubstring(fastaSeq,potential_repeat_seq,potential_repeat_end);
    //       final int lineWhereThisFlagIn=Math.ceil(thisThreePrimeAbsStart/60)+1;
    //       final int thisThreePrimeAbsStartInLine=thisThreePrimeAbsStart-(lineWhereThisFlagIn-1)*60;

    //       List<Long> possibleNextThreePrimeLine=threePrimeRegions.filter(new Function<Long, Boolean>(){
    //         public Boolean call(Long lineNum){
    //            return(lineNum.longValue()>=lineWhereThisFlagIn)
    //         }
    //       }).collect();
          
    //       for(int m=0;m<possibleNextThreePrimeLine.size();m++){
    //            long thatLine=possibleNextThreePrimeLine.get(m).longValue();
    //            int thisLineAbsStart=60*(thatLine-1);
    //            int thisLineAbsEnd=60*(thatLine-1)+60;
    //            String thisLineText=getSubstring(fastaSeq,thisLineAbsStart,thisLineAbsEnd);
    //            boolean continueAnalysis=true;
    //            int start_idx_1=0;
    //            int start_idx_2=0;
    //            while(continueAnalysis){
    //               int idx_1=thisLineText.indexOf("GAAAG",start_idx_1);
    //               int idx_2=thisLineText.indexOf("GAAAC",start_idx_2);

    //               if(idx_1>=0){                                                           
    //                 int primeAbsStart=thisLineAbsStart+idx_1-1;             
    //                 int possibleNextRepeatStart=primeAbsStart-max_repeat_size;
    //                 int possibleNextRepeatEnd=primeAbsStart-1;
    //                 String possibeNextRepat=getSubstring(fastaSeq,possibleNextRepeatStart,possibleNextRepeatEnd);

    //                 start_idx_1=idx_1+5;

    //               }
    //               else{
    //                         if(idx_2>=0){
    //                             threePrimeAbsLocs.add(60*(thisLine-1)+idx_2+1) ;                             
    //                             start_idx_2=idx_2+5;
    //                         }

    //                         else{
    //                             continueAnalysis=false;
    //                         }                               
    //               }
    //            }

    //       }



     

    //     }
    //     return(repeatPrimeAbsStartLoc);
    // }

    public String getSubstring(List<String> seqFile, int start_loc, int end_loc){
        int startLine=(int)Math.ceil(start_loc/60)+1;
        int endLine=(int)Math.ceil(end_loc/60)+1;
        int startLocIdxInLine=start_loc-(startLine-1)*60-1;
        int endLocIdxInLine=end_loc-(endLine-1)*60-1;
        
        
        String result="";
        String part=seqFile.get(startLine);
        
        if(startLine==endLine){
           result=part.substring(startLocIdxInLine,endLocIdxInLine+1);
        }
        else{
         
           result=part.substring(startLocIdxInLine,part.length())+seqFile.get(endLine).substring(0,endLocIdxInLine+1);
        }

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
            transformedValue=2;
          }
          if(thisLetter.equals('G')){
            transformedValue=-2;
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



 }



