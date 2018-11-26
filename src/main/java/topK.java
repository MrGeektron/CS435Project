
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class topK{
	
	static String delim_CSV = ",";
	static final String TOP_K = "10";

	public static class Map1 extends Mapper<Object, Text, Text, NullWritable> {
	
		TreeMap<String, Double> topNdrugs = new TreeMap<String, Double>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			int N = 10;
			String[] line, amounts;
			boolean TargetTypeState;
			Double total_amount = 0.0;
			
			try {
				N = Integer.parseInt(context.getConfiguration().get(TOP_K).toString());
			} catch (Exception e) {
				System.out.println("3rd arg must be an integer, using N defaul value (10)");
			}

			//Check if second element is STATE to process accordingly
			try {
				Double.parseDouble(value.toString().split(delim_CSV, 3)[1]);
				TargetTypeState = false;
			} catch (Exception e) {
				TargetTypeState = true;
			}
			
//			System.out.println(key.toString() + " --- " + value.toString());
			
			if(TargetTypeState) {
				line = value.toString().split(delim_CSV, 3);
				amounts = line[2].split(delim_CSV);
			}else {
				line = value.toString().split(delim_CSV, 2);
				amounts = line[1].split(delim_CSV);
			}
			
			for (int i = 0; i < amounts.length; i++) {
				total_amount += Double.parseDouble(amounts[i]);
			}
			
			topNdrugs.put(value.toString(), total_amount);

			//topK of words comparing values (instead of keys)
			if (topNdrugs.size() > N) {
				Entry<String, Double> min = null;
				for (Entry<String, Double> entry : topNdrugs.entrySet()) {
				    if (min == null || min.getValue() > entry.getValue())
				        min = entry;
				}
				topNdrugs.remove(min.getKey());
			}
		}
		
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Double> entry : topNdrugs.entrySet()) 
				context.write(new Text(entry.getKey()), NullWritable.get());
		}
		
	}
	

	public static void main(String[] args) throws Exception {
		/*
		 * args: <trend_path> K
		 * 1º arg >
		 * trend_path: path for the some drug trend
		 * 2º arg > 
		 * K: amount of top values to output
		 * e.g: "trend_opioid_national 15" will create a file with only the first top 15 drugs from
		 * the file "trend_opioid_national" and save it as "topK_trend_opioid_national"
		*/
		
		//folders to organize the directory
		String resF = "res/";
		String outF = resF + "output/";
		String inPath = outF + args[0];
		String outPath = outF + "topK_" + args[0];

		Configuration conf = new Configuration();

		conf.set(TOP_K, args[1]);
		
		FileUtils.deleteDirectory(new File(outPath));
		
		Job job1 = Job.getInstance(conf, "topK drugs");
		job1.setJarByClass(topK.class);
		job1.setMapperClass(Map1.class);
		job1.setNumReduceTasks(0);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inPath));
		FileOutputFormat.setOutputPath(job1, new Path(outPath));	
		

		System.exit(job1.waitForCompletion(true) ? 0 : 1);

	}
}