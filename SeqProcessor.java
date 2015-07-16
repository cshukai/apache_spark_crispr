import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.io.Serializable;
import java.util.*;
import scala.Tuple2;


public class SeqProcessor implements Serializable{

	public static void main(String [ ] args) throws Exception{
		SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
		JavaSparkContext sc=new JavaSparkContext(conf); 		
		SeqProcessor proc=new SeqProcessor();
		final double cutoff=0.70;

		JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Xanthomonas_campestris_pv_campestris_str_atcc_33913.GCA_000007145.1.26.dna.chromosome.Chromosome.fa");
		JavaPairRDD<String,Long>  freq_region=proc.computeRegionFract(inputs,'A','T',cutoff);
        JavaRDD<Long[]> possibleLeadRegion=sc.parallelize(proc.flagLeadSeq(freq_region));
        
       
        // System.out.println("total count:"+possibleLeadRegion.count());
        // List<Long[]> testList=possibleLeadRegion.collect();
        // for(int i=0;i<testList.size();i++){
        //     Long[] this_array=testList.get(i);
        //     System.out.println("first:"+this_array[0]+"second:"+this_array[1]);
        // }
        List<Long> full_regions=freq_region.lookup("11");
        List<Long> left_rich_regions=freq_region.lookup("10");
        List<Long> right_rich_regions=freq_region.lookup("01");

        for(int i=0;i<full_regions.size();i++){
            System.out.println("full:"+full_regions.get(i));
        }
         for(int i=0;i<left_rich_regions.size();i++){
            System.out.println("left:"+left_rich_regions.get(i));
        }

         for(int i=0;i<right_rich_regions.size();i++){
            System.out.println("right:"+right_rich_regions.get(i));
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


     // todo : add a method for string matching
 }



