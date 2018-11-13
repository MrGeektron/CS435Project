
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class simpleProcessing {

	 

	public static class map1 extends Mapper<Object, Text, Text, DoubleWritable>{
		


		
		public void map(Object key, Text value, Context context	) throws IOException, InterruptedException {
			
			String delim_CSV = ",";
			String[] entry = value.toString().split(delim_CSV);
			String state = entry[1];
			String supress = entry[8];
			String NDC = entry[19];
			String year = entry[5];
			String amount = entry[9];

			if(supress.equals("false")) {
//				System.out.println(year+"   "+amount);
				context.write(new Text(year), new DoubleWritable(Double.parseDouble(amount)));
			}
		}
	}
	
	public static class red1 extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			System.out.println("reducer");

			for (DoubleWritable val : values) {
				sum += val.get();
			}
			System.out.println(key.toString()+"   "+sum);

			context.write(key, new DoubleWritable(sum));
			
		}
	}
	

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TermProject");
		job.setJarByClass(simpleProcessing.class);
		job.setMapperClass(map1.class);
		job.setReducerClass(red1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}