import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.io.Serializable;
import java.util.*;


public class SeqProcessor implements Serializable{

	public static void main(String [ ] args) throws Exception{
		SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("spark://masterb.nuc:7077");
		JavaSparkContext sc=new JavaSparkContext(conf); 		
		SeqProcessor proc=new SeqProcessor();
		JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Xanthomonas_campestris_pv_campestris_str_atcc_33913.GCA_000007145.1.26.dna.chromosome.Chromosome.fa");
		//proc.findDiNuRich(inputs);
	}

	public JavaPairRDD findDiNuRich(JavaRDD<String>  seqFiles){
        
        JavaRDD<Integer[]> seqFiles=seqs.flatMap(new FlatMapFunction<String,Integer[]> (){
        	public Iterable<Integer[]> call(String line) {
        		ArrayList<Integer[]> result = new ArrayList<Integer[]>();                               
                line=line.toUpperCase();
                // 0-left 1-right of a line
                int left_hit=0;
                int right_hit=0;
                int seq_length=line.length();
                Integer[] result_this_line=new Integer[2];
                for(int i=0;i<seq_length;i++){
                	
                	Character thisLetter=line.charAt(i);     
                	if(thisLetter.equals('A')||thisLetter.equals('T')){
                		if(i>(seq_length/2)){
                			right_hit=right_hit+1;
                		}
                		else{
                			left_hit=left_hit+1;
                		}
                	}
                    
                    

                	
                }
                result_this_line[0]=left_hit;
                result_this_line[1]=right_hit;

                result.add(result_this_line);

       			return result;
        	}
        }); 
  		return (di_rich_regions.zipWithIndex());


	}



}



