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
		JavaPairRDD<Double[],Long>  freq_region=proc.computeATfreq(inputs);
		JavaPairRDD<Double[],Long>  possible_leader_region=proc.getATrichRegions(freq_region,cutoff);
		JavaRDD<Double[]> test=possible_leader_region.keys();
		JavaRDD<Long> rows=possible_leader_region.values();
		Double[] test_arry=test.first();
		System.out.println("left:"+test_arry[0]+"right:"+test_arry[1]);
		System.out.println("first row "+rows.first());
	}

	public JavaPairRDD<Double[],Long> computeATfreq(JavaRDD<String>  seqFiles){
        
        JavaRDD<Double[]> di_rich_regions=seqFiles.flatMap(new FlatMapFunction<String,Double[]> (){
        	@Override
        	public Iterable<Double[]> call(String line) {
        		ArrayList<Double[]> result = new ArrayList<Double[]>();                               
                line=line.toUpperCase();
                // 0-left 1-right of a line
                Double left_hit=0.00;
                Double right_hit=0.00;
                double seq_length=line.length();
                Double[] result_this_line=new Double[2];
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
                result_this_line[0]=left_hit/(seq_length/2);
                result_this_line[1]=right_hit/(seq_length/2);

                result.add(result_this_line);

       			return result;
        	}
        }); 

  //       Double[] test=di_rich_regions.first();
		// System.out.println("test:"+test[0]+" "+test[1]);

  		return (di_rich_regions.zipWithIndex());


	}

	//possibleLeadSeqs  String [left-freq, right-freq], lineNum
    public  JavaPairRDD<Double[],Long> getATrichRegions(JavaPairRDD<Double[],Long> freq_region ,double cutoff){
    	final double cut=cutoff;

  		
  		JavaPairRDD<Double[],Long> result=freq_region.filter(
  				new Function<Tuple2<Double[], Long>, Boolean>() {
    				@Override
    				public Boolean call(Tuple2<Double[], Long> keyValue) {
    					Double[] thisATfreq=keyValue._1();
    					Long rowNum=keyValue._2();
                        return (!(thisATfreq[0]<cut && thisATfreq[1]<cut) && !rowNum.equals("0") );
    				}
  				}

  			);
        return(result);
    }



 }



