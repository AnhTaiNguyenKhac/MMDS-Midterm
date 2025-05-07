package midterm;
import java.io.IOException; 
import java.util.StringTokenizer;
import java.lang.Integer;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class AprioriPass2 {

	public static class FilterMapper extends 
							Mapper<Object, Text, Text, IntWritable> {

		private Set<String> frequentItems = new HashSet<>();
		private final static IntWritable one = new IntWritable(1);

		// support method
		// convert array of Strings (Basket) to HashSet
		public HashSet<String> getBasket(String[] items){

			return new HashSet(Arrays.asList(items));
		}

		// support method
		public HashSet<String> fqItemsInBasket(HashSet<String> basket) {
			basket.retainAll(this.frequentItems);
			return basket;
		}


		// setup method always run in a MapTask and before doing map
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();

			if(cacheFiles != null && cacheFiles.length > 0) {
				try {
					FileSystem fs = FileSystem.get(context.getConfiguration()); 

	                Path getFilePath = new Path(cacheFiles[0].toString()); 

					BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(getFilePath)));

					String line = "";
					while((line = reader.readLine()) != null){
						// each line is formatted as: item \t its count
						this.frequentItems.add(line.split("\t")[0].trim());
					}

					reader.close();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context) 
						throws IOException, InterruptedException{
			// value: ("") i1,i2,i3,...
			try {
				// create HashSet basket
				HashSet<String> basket =  this.getBasket(
							value.toString().trim()
							.replaceAll(",\\s+", ",").split(",")); // avoid the space after comma

				// get frequent items in the basket
				HashSet<String> freqItemsInBasket = this.fqItemsInBasket(basket);


				// convert HashSet to String array
				String[] itemsArr =  new String[basket.size()];
				basket.toArray(itemsArr); // now, itemsArr contains fq. items in the basket


				// generate pairs by frequent items in the basket
				// also ignore the case that intersection has no items
				for (int i = 0; i < itemsArr.length; i++) {
		            for (int j = i + 1; j < itemsArr.length; j++) {
		                context.write(
		                	new Text("{" + itemsArr[i] + ", " + itemsArr[j] + "}"), one);
		            }
		        }
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}


	public static class ConstructorReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val: values)
				sum += val.get();
			if(sum >= context.getConfiguration().getInt("supportThreshold", 1)){
				context.write(key, new IntWritable(sum));
			}
		}
	}


	public static void main(String args[]) throws Exception {
		Configuration conf =  new Configuration();
		// 3rd is the support theshold
		conf.setInt("supportThreshold", Integer.parseInt(args[2]));

		Job job = Job.getInstance(conf, "Apriori Pass 2");

		
		// 4th arg is the path of broadcase file whose content is frequent items on HDFS
		// that is part-r-00000 that also is the result in pass 1 of Apriori algorithm
		// it is formatted as [frequent item] [\t] [its count]
		job.addCacheFile(new URI(args[3]));


		job.setJarByClass(AprioriPass2.class);
		job.setMapperClass(FilterMapper.class);
		job.setCombinerClass(ConstructorReducer.class);
		job.setReducerClass(ConstructorReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}