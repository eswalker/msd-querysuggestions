package com.eswalker.msd.QuerySuggestions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class QuerySuggestions extends Configured implements Tool{
	public static class HMapper extends Mapper<LongWritable, Text, Text, Text> {	     
		
		private static Text outputKey = new Text();
	    private static Text outputValue = new Text();
	   
	    private static int tagStartIndex = 5;
	   
	    @Override
	    protected final void setup(final Context context) throws IOException, InterruptedException {
	  
	    }

		@Override
		public final void map(final LongWritable key, final Text value, Context context) throws IOException, InterruptedException {
			
			String data[] = value.toString().split("\\|");
			
			/* Get and Store Tags */
			HashSet<String> tags = new HashSet<String>();
			for (int i = tagStartIndex; i < data.length; i++) {
				String tagAndScore = data[i];
				String data2[] = tagAndScore.split(",");
				if (data2.length == 2) {
					String tag = data2[0];
					tags.add(tag);
				}
			}
			
			/* For each tag, tokenize by whitespace and output the partial tags as keys */
			for (String _tag : tags) {
				
				outputValue.set(_tag);
				
				data = _tag.split("\\s");
				for (int i = 0; i < data.length; i++) {
					for (int j = 1; j <= data[i].length(); j++) {
						outputKey.set(data[i].substring(0,j));
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}

	
	public static class HReducer extends Reducer<Text, Text, NullWritable, Text> {
		private static NullWritable nullKey = NullWritable.get();
        private static Text outputValue = new Text();
        
        
        public class TagCount implements Comparable<TagCount> {
        	public String tag;
        	public int count = 0;
        	public TagCount(String tag) 		{ this.tag = tag; }
			public int compareTo(TagCount o) 	{ return o.count - this.count; }
			public void increment() 			{ count++; }
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	HashMap<String, TagCount> map = new HashMap<String, TagCount>();
        	
        	for (Text t : values) {
        		String tag = t.toString();
        		if (!map.containsKey(tag)) 
        			map.put(tag, new TagCount(tag));
        		map.get(tag).increment();
        	}
        	
        	int size = map.size();
        	TagCount[] array = new TagCount[size];
        	int i = 0;
        	for (String _tag : map.keySet()) {
        		array[i++] = map.get(_tag);
        	}
        	Arrays.sort(array);
        	
        	StringBuilder sb = new StringBuilder();
        	sb.append(key.toString());		sb.append('|');
        	for (i = 0; i < array.length; i++) {
        		sb.append(array[i].tag) ; 		sb.append('|');
        	}
        	sb.deleteCharAt(sb.length()-1);
        	
        	outputValue.set(sb.toString());
        	context.write(nullKey, outputValue);
        }
        
    }

    /**
     * Sets up job, input and output.
     * 
     * @param args
     *            inputPath outputPath
     * @throws Exception
     */
	public int run(String[] args) throws Exception {

       Configuration conf = getConf();
    	
        
       Job job = new Job(conf);
       job.setJarByClass(QuerySuggestions.class);
       job.setJobName("QuerySuggestions");


       job.setInputFormatClass(TextInputFormat.class);
       


       TextInputFormat.addInputPaths(job, args[0]);
       TextOutputFormat.setOutputPath(job, new Path(args[1]));


       job.setMapperClass(HMapper.class);
       job.setReducerClass(HReducer.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);

       return job.waitForCompletion(true) ? 0 : 1;
    }
	
    public static void main(String args[]) throws Exception {
    	
        if (args.length < 2) {
            System.out.println("Usage: QuerySuggestions <input dirs> <output dir>");
            System.exit(-1);
        }
 
        int result = ToolRunner.run(new QuerySuggestions(), args);
        System.exit(result);
        
   }


}

