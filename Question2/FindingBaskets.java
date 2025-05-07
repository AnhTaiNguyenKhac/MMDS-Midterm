package midterm;
// if you compile and run this file that in outside or parent folder of midterm/FindingBaskets.java, 
// please write package (eg: package midterm on above)
import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FindingBaskets {

    public static class CreateKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text basketId = new Text();
        private Text item = new Text();
        @Override
		public void map(LongWritable key, Text value, Context context) 
                            throws IOException, InterruptedException {
			try {

                // ignore header, its key is the offset=0
                // to make sure check that whether the line contains substring "Member_number"
                if(key.get() != 0 && !value.toString().contains("Member_number")) {

                    String[] fields = value.toString().split(",");

                    // basketId is the combination between Member_number and date
                    // item is the itemDescription
                    basketId.set(fields[0] + "-" + fields[1]);
                    item.set(fields[2]);
                    context.write(basketId, item);
                }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

    public static class ConvertStringReducer extends Reducer<Text, Text, Text, Text> {

        // convert the array of items to string that formated as item1, item2, ...
        @Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
						throws IOException, InterruptedException {
            
            // using HashSet to only get distince items in values
            HashSet<String> uniqueItems = new HashSet<>();               
            
            for(Text val : values)
                uniqueItems.add(val.toString());

            // convert to ArrayList is just for easily sorting
            ArrayList<String> basket = new ArrayList<String>(uniqueItems);

            // using sort for getting the consistancy for further step, e.g: generate pairs
            Collections.sort(basket);

            // convert it to string: "[item1, item2, ...]"
            // later, get the string for idx 0->n-1 to ignore 2 brackets "[" "]"
            String stringBasket = basket.toString();

            // key is empty, could try to put "key" parameter
            // in the value, format i1, i2,... -> i1,i2,... (no space after comma)
            // avoid the step trim() for each element in array for further step
			context.write(new Text(""), new Text(stringBasket
                                                    .substring(1, stringBasket.length() - 1)
                                                    .replaceAll(",\\s+", ",")));
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Finding Baskets");
        job.setJarByClass(FindingBaskets.class);

        job.setMapperClass(CreateKeyMapper.class);
        // if using combiner by ConvertStringReducer class, 
        // keep the "key" parameter (i.e: don't set it like "")
        // bc. combiner needs key to do group by key
        // reducer receives the key-values whose values that grouped by key in combiner
        // job.setCombinerClass(ConvertStringReducer.class);
        job.setReducerClass(ConvertStringReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
