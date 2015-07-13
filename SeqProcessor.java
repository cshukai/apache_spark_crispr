import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.io.Serializable;


public class SeqProcessor implements Serializable{

	public static void main(String [ ] args) throws Exception{
		SparkConf conf=new SparkConf().setAppName("spark-crispr").setMaster("local[2]");
		JavaSparkContext sc=new JavaSparkContext(conf); 		
		SeqProcessor proc=new SeqProcessor();
		JavaRDD<String> inputs=sc.textFile("bacteria/crispr/data/Xanthomonas_campestris_pv_campestris_str_atcc_33913.GCA_000007145.1.26.dna.chromosome.Chromosome.fa");
		proc.subSeqFasta(inputs);
	}

	public void subSeqFasta(JavaRDD<String>  seqFiles){
		
		JavaRDD<Integer> lineLengths = seqFiles.map(new Function<String, Integer>() {
			
			public Integer call(String s) {
			   if(s.contains(">")){
			   	   return 0;
			   } 

			   else{
			   		return s.length();
			   }

			}
		});
		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		System.out.println("total length is"+totalLength);

	}


}


