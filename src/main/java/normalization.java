
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class normalization{
	
	static String delim_CSV = ",";

	public static class Map1 extends Mapper<Object, Text, Text, NullWritable> {
	

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line, amounts;
			boolean TargetTypeState;
			Double total_amount = 0.0;
			String head;


			//Check if second element is STATE to process accordingly
			try {
				Double.parseDouble(value.toString().split(delim_CSV, 3)[1]);
				TargetTypeState = false;
			} catch (Exception e) {
				TargetTypeState = true;
			}
			
			if(TargetTypeState) {
				line = value.toString().split(delim_CSV, 3);
				head = line[0]+delim_CSV+line[1];
				amounts = line[2].split(delim_CSV);
			}else {
				line = value.toString().split(delim_CSV, 2);
				head = line[0];
				amounts = line[1].split(delim_CSV);
			}
			
			for (int i = 0; i < amounts.length; i++) {
				total_amount += Double.parseDouble(amounts[i]);
			}
			
			StringBuilder amounts_norm = new StringBuilder();

			for (String a : amounts) {
				amounts_norm.append(Double.parseDouble(a)/total_amount*100.0);
				amounts_norm.append(delim_CSV);
			}   
			
			context.write(new Text(head+delim_CSV+amounts_norm), NullWritable.get());

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
		String outPath = outF + "normalize/" + args[0];
		Configuration conf = new Configuration();

		
		FileUtils.deleteDirectory(new File(outPath));
		
		Job job1 = Job.getInstance(conf, "normalize drugs");
		job1.setJarByClass(normalization.class);
		job1.setMapperClass(Map1.class);
		job1.setNumReduceTasks(0);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inPath));
		FileOutputFormat.setOutputPath(job1, new Path(outPath));	
		job1.waitForCompletion(true);
		System.out.println("Finish");
		System.exit(1);

	}
}