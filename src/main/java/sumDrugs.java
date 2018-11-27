
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class sumDrugs{
	
	static String delim_CSV = ",";
	static final String TOP_K = "10";

	public static class Map1 extends Mapper<Object, Text, Text, DoubleWritable> {
	

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line, amounts;
			String state;
			boolean TargetTypeState;

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
				state = line[1];
				amounts = line[2].split(delim_CSV);

			}else {
				line = value.toString().split(delim_CSV, 2);
				state = "national";
				amounts = line[1].split(delim_CSV);
			}
			int i = 0;
			for (int y = 1992; y < 2019; y++) {
				context.write(new Text(y+delim_CSV+state), new DoubleWritable(Double.parseDouble(amounts[i])));
				i++;
			}
		}	
	}
	
	public static class Red1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
//			System.out.println("Running RED from Job1");

			Double amount =0.0;
			
			for (DoubleWritable val : values) {
				amount += Double.parseDouble(val.toString());
			}   
			context.write(key, new DoubleWritable(amount));
		}
	}
	
	public static class Map2 extends Mapper<Object, Text, Text, Text> {
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line [] = value.toString().split("	");
			String state =line[0].split(delim_CSV)[1];
			String amount = line[1];

			context.write(new Text(state), new Text(amount));
		}	
	}
	
	public static class Red2 extends Reducer<Text, Text, Text, NullWritable> {


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder amounts = new StringBuilder();

			amounts.append(key.toString());
			amounts.append(delim_CSV);

			for (Text val : values) {
				amounts.append(val);
				amounts.append(delim_CSV);
//				amounts.insert(0,val);
//				amounts.insert(0,delim_CSV);
			}   
			amounts.deleteCharAt(amounts.length()-1);
			context.write(new Text(amounts.toString()), NullWritable.get());
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
		String outF = resF + "out_cluster/";
		String inPath = outF + args[0];
		String outPath = outF + "topK_/" + "sum_" + args[0];
		String pathJ1= "aux_sumDrugs";

		Configuration conf = new Configuration();
		
		FileUtils.deleteDirectory(new File(pathJ1));
		FileUtils.deleteDirectory(new File(outPath));
		
		Job job1 = Job.getInstance(conf, "sum drugs j1");
		job1.setJarByClass(sumDrugs.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Red1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inPath));
		FileOutputFormat.setOutputPath(job1, new Path(pathJ1));	
		job1.waitForCompletion(true);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);


		///////////////////////   JOB 2    /////////////////////////////////////////////////

		Job job2 = Job.getInstance(conf, "sum drugs j2");
		job2.setJarByClass(sumDrugs.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Red2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job2, new Path(pathJ1));
		FileOutputFormat.setOutputPath(job2, new Path(outPath));
//		job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}