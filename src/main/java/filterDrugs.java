
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class filterDrugs {

	static String delim = "	";
	static String delim_CSV = ",";
	static String pad_zeros = "^0+(?!$)";
	static boolean NDCparts = false; // flag if we want the NDC to be splitted with a delimiter
	static final String DRUG_TYPE_ARG = "drugType";
	static final String DRUG_TYPE_O = "opioid";
	static final String DRUG_TYPE_NO = "nonopioid";
	static final String TARGET_TYPE_ARG = "targetType";
	static final String TARGET_TYPE_N = "national";
	static final String TARGET_TYPE_S = "state";
	
	public static class Map1 extends Mapper<Object, Text, Text, DoubleWritable> {

		HashMap<String, String> lookUp = new HashMap<String, String>();

		// Create a lookup table with the cache file uploaded 
		public void setup(Context context) throws IOException, InterruptedException {
//			System.out.println("Running SETUP from Job1");
			FileSystem fs = FileSystem.get(context.getConfiguration());  

			String drugType = context.getConfiguration().get(DRUG_TYPE_ARG).toString();

			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI[] files = context.getCacheFiles();
				// Read all files in the DistributedCache
				for (URI u : files) {
					BufferedReader rdr = new BufferedReader(
							new InputStreamReader(fs.open(new Path(u.toString()))));
					String line = null;

					// For each record in the file
					while ((line = rdr.readLine()) != null) {
						// Get the user NDC and the rest for this entry

//						In case we want to do a deeper parsing
//						String[] entry = line.split(delim_CSV);
//						String NDC = entry[0];
//						String drug = entry[1];
//						String SPU = entry[2];
//						String MME = entry[3];      

						// key: "NDC"; value: "Drug,Strength_Per_Unit,MME_Conversion_Factor"
						if (drugType.equals(DRUG_TYPE_O)) {
							String[] entry = line.split(delim_CSV, 2);
							lookUp.put(entry[0], entry[1]);
						} else {
							String[] entry = line.split(delim_CSV, 3);
							if (NDCparts)
								lookUp.put(entry[1].replaceAll("\"", ""), entry[2]);
							else
								lookUp.put(entry[1].replaceAll("-|\"", ""), entry[2]);
						}
					}
					rdr.close();
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println("Running MAP from Job1");

			String drugType = context.getConfiguration().get(DRUG_TYPE_ARG).toString();
			String NDC = null;

			String NDC_delim = "-";
			if (!NDCparts)
				NDC_delim = "";

			if (((LongWritable) key).get() == 0)
				return; // skips header. It's important to do it so we can easily get NDC without
						// concerning of commas used within a field

			String[] entry = value.toString().split(delim_CSV);
			String state = entry[1];
			String supress = entry[8];
			String amount = entry[9];
			String year = entry[5];

			//some years have swapped columns 8 and 9
			if(year.matches(".*1995|1998|1999|2000|2002|2007|2008|2010.*")) {
				supress = entry[9];
				amount = entry[8];
			}

			if (supress.equals("true") || state.equals("XX"))
				return;

			try {
				Double.parseDouble(amount);
			} catch (Exception e) {
				return;
			}
			//System.out.println("amount: "+amount+", supress: "+supress);
			
			// NDC can be retrieve from col 19 but the three parts are padded with zeros
			// which make it impossible to look for.
			// also, col 18 has a coordinate in the form "(y,x)", so we can use entry[20]
			// and if we want to be more tidy and go for 19 we should use
			// \s*("[^"]*"|[^,]*)\s*
			// Sometimes is better to create NDC merging all parts removing in forehand
			// the padding zeros
			if (drugType.equals(DRUG_TYPE_O))
				NDC = entry[20].replaceFirst(pad_zeros, "");
			else
				NDC = entry[2].replaceFirst(pad_zeros, "") + NDC_delim + entry[3].replaceFirst(pad_zeros, "")
						+ NDC_delim + entry[4].replaceFirst(pad_zeros, "");



			// for each NDC there more than 1 entry, we these two fields changing. Thus, we
			// need a reducer to sum up the 4 values
			// String quarter = entry[6]; // 1, 2, 3 and 4
			// String utilType = entry[0]; // FFSU and MCOU

			// which will only happen when data is available (and also skips header if not
			// done before)
			if (lookUp.containsKey(NDC)) {
				context.write(new Text(NDC + delim_CSV + state + delim_CSV + year),
						new DoubleWritable(Double.parseDouble(amount)));
			}
		}

	}

	public static class Red1 extends Reducer<Text, DoubleWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
//			System.out.println("Running RED from Job1");

			double sum = 0;

			// aggregate all the occurrences of a drug
			for (DoubleWritable val : values)
				sum += val.get();

			context.write(new Text(key + delim_CSV + sum), NullWritable.get());
		}
	}

	public static class Map2 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println("Running MAP from Job2");

			String targetType = context.getConfiguration().get(TARGET_TYPE_ARG).toString();

			String[] entry = value.toString().split(delim_CSV);
			String NDC = entry[0];
			String state = entry[1];
			String year = entry[2];
			String amount = entry[3];

			if(targetType.equals(TARGET_TYPE_N)) //in case of TargetType=national reduce just by drug
				context.write(new Text(NDC), new Text(year + delim_CSV + amount));
			else { //in case of TargetType=state or misspell reduce by drug and state
				if(!targetType.equals(TARGET_TYPE_S)) //if misspell just give a warning but don't abort
					System.out.println("WARNING: wrong argument for Target Type. Processing as State");
				context.write(new Text(NDC + delim_CSV + state), new Text(year + delim_CSV + amount));
			}
		}
	}

	public static class Red2 extends Reducer<Text, Text, Text, NullWritable> {

		String id_prev = new String();
		int max_per_article = 0;
		String MAXk_Fkj_identifier = "$MAXk_Fkj$";

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//			System.out.println("Running RED from Job2");

			String targetType = context.getConfiguration().get(TARGET_TYPE_ARG).toString();

			String[] year_amount;
			int year;
			double amount;
	        StringBuilder timeSeriesCSV = new StringBuilder();
			TreeMap<Integer, Double> timeSerieTree = new TreeMap<Integer, Double>();

			//Initialization of treeMap with all years with value 0.
			//It will give the right order to all amounts no matter if a drug doesn't appear in one year
			//It will allow to simplify the code for the aggregation in case of targetType=state
			for (int y = 1992; y < 2019; y++) 
				timeSerieTree.put(y, 0.0);
			
			
			for (Text val : values) {
				year_amount = val.toString().split(delim_CSV);
				year = Integer.parseInt(year_amount[0]);
				amount = Double.parseDouble(year_amount[1]);

				if(targetType.equals(TARGET_TYPE_N)) {//in case of TargetType=national aggregate all repeated years' amount
					if(timeSerieTree.containsKey(year))
						timeSerieTree.put(year, amount + timeSerieTree.get(year));
				}else //in case of TargetType=state or misspell (handle also in the Map2)...
					timeSerieTree.put(year, amount);
			}
			
			//create a CSV string from the treeMap
			for (Entry<Integer, Double> entry : timeSerieTree.entrySet())
				timeSeriesCSV.append(delim_CSV + entry.getValue());
			
			context.write(new Text(key.toString()+timeSeriesCSV.toString()), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		 * args: <database_path> <opioid|nonopioid> <national|state>  <<output_path> 
		 * 1º arg >
		 * database_path: path for the medicaid database 
		 * 2º arg > 
		 * opioid or nonopioid: the name of the folder containing all csv of each class must match these two
		 * names, and is also use for the output folder in case it is not specified 
		 * 3º arg > 
		 * national or state: change the final output to a trend of drug nationwide or statewide
		 * 4º arg >
		 * output_path: optional argument for the output folder name/location 
		 * e.g: "medicaid_2018_CO.csv opioid" will create a file under "medicaid_opioids"
		 * with all the opioids contained in the medicaid_2018_CO.csv
		 */
		Configuration conf = new Configuration();
		
		//folders to organize the directory
		String resF = "res/";
		String inF  = resF + "input/";
		String outF = resF + "output/";
		
		conf.set(DRUG_TYPE_ARG, args[1]);
		String drugType = conf.get(DRUG_TYPE_ARG).toString();
		System.out.println("Drug type: " + drugType);
		
		conf.set(TARGET_TYPE_ARG, args[2]);
		String targetType = conf.get(TARGET_TYPE_ARG).toString();
		System.out.println("Target type: " + targetType);

		
		String outPath = null;
		String pathJ1 = null;

		if (args.length == 3) { // if no output file was given..
				outPath = outF + "trend_" + drugType + "_" + targetType;
				pathJ1  = outF + "filtered_" + drugType;
		}else if (args.length == 4)
			outPath = args[3];
		else {
			System.out.println("args: <database_path> <opioid|nonopioid> <national|state> <<output_path>>");
			System.exit(0);
		}

		FileUtils.deleteDirectory(new File(outPath));
		FileUtils.deleteDirectory(new File(pathJ1));

		/////////////////////// JOB 1 /////////////////////////////////////////////////
		/*
		 * OUTPUT in CSV format with fields: NDC,STATE,YEAR,AMOUNT
		 * e.g.: 65162004710,DC,2017,200.0
		 */
		Job job1 = Job.getInstance(conf, "filter_drugs_by_type");
		job1.setJarByClass(filterDrugs.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Red1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job1, new Path(inF+args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(pathJ1));

		FileInputFormat.setInputDirRecursive(job1, true);
		FileSystem fs = FileSystem.get(conf);  
		RemoteIterator<LocatedFileStatus> fIterator = fs.listFiles(new Path(inF+args[1]), true);
		 while(fIterator.hasNext()){
		        LocatedFileStatus fileStatus = fIterator.next();
		        //do stuff with the file like ...
		        job1.addFileToClassPath(fileStatus.getPath());
		    }
		
		//loading to cache all files with lists of the same drug type
//		for (final File fileEntry : new File(inF+args[1]).listFiles()) {
//			System.out.println("Loading to cache: " + fileEntry.getName());
//			job1.addCacheFile(new Path(fileEntry.getPath()).toUri());
//		}
		// job1.addCacheFile(new URI("hdfs://server:port/FilePath/part-r-00000"));
		job1.waitForCompletion(true);
		System.out.print("Finish job1, ");
		System.out.println("Output directory: " + pathJ1);

		/////////////////////// JOB 2 /////////////////////////////////////////////////
		/*
		 * OUTPUT in CSV format with fields depending on argument TargetType:
		 * 
		 * case TargetType=national: NDC,AMOUNT_1991,AMOUNT_1992,...,AMOUNT_2018
		 * e.g.: 65162004710,300.0,500.0,...,200.0
		 * 
		 * case TargetType=national: NDC,STATE,AMOUNT_1991,AMOUNT_1992,...,AMOUNT_2018
		 * e.g.: 65162004710,NY,100.0,200.0,...,50.0
		 */
		Job job2 = Job.getInstance(conf, "build_time_series");
		job2.setJarByClass(filterDrugs.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Red2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job2, new Path(pathJ1));
		FileOutputFormat.setOutputPath(job2, new Path(outPath));
		job2.waitForCompletion(true);

		job1.waitForCompletion(true);
		System.out.print("Finish job2, ");
		System.out.println("Output directory: " + outPath);

		System.exit(0);
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}