package midterm;
import java.io.IOException; 
import java.util.StringTokenizer;
import java.lang.Integer;

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

public class AprioriPass1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text item =  new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try {
				StringTokenizer itr = new StringTokenizer(value.toString().trim(), ",");
				while(itr.hasMoreTokens()){
					item.set(itr.nextToken().trim());
					context.write(item, one);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
			

		}
	}

	// this class is also for visualizing, the result is "individual-item \t its count" (the count >= supportThreshold)

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val: values)
				sum += val.get();
			if(sum >= context.getConfiguration().getInt("supportThreshold", 1)){
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf =  new Configuration();
		conf.setInt("supportThreshold", Integer.parseInt(args[2]));
		Job job = Job.getInstance(conf, "Apriori Pass 1");

		job.setJarByClass(AprioriPass1.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}