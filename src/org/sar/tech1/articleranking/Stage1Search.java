
package org.sar.tech1.articleranking;



import java.io.IOException;

import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Stage1Search extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(Stage1Search.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new Stage1Search(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		Path output = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}


		Job job = Job.getInstance(config, "Stage1Search");
		job.setJarByClass(this.getClass());

		job.setMapperClass(Map2.class);
		job.setReducerClass(Reduce2.class);

		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map2.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map2.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map2 extends
	Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			//LOG.info(data+"HEreeeeee");
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){	

				String record3[]=record.split("\t");
				if((StringUtils.countMatches(record3[1], Constant.SEPARATOR)>2))
				{
					String record2[]=record3[1].split(Constant.SEPARATOR);
					context.write(new IntWritable(Integer.parseInt(record3[0])), new Text(record2[0]+Constant.TITLE_IDENTIFIER));

				}
				else if((StringUtils.countMatches(record3[1], Constant.SEPARATOR)==2))
				{
					context.write(new IntWritable(Integer.parseInt(record3[0])), new Text(record3[1]));
				}

			}

		}
	}

	public static class Reduce2 extends
	Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			StringBuffer myBuffer=new StringBuffer();			
			StringBuffer titleBuffer=new StringBuffer();
			StringBuffer paperscoreBuffer=new StringBuffer();
			for(Text value:valueList){
				if(value.toString().contains(Constant.TITLE_IDENTIFIER))
				{
					titleBuffer.append(value.toString().replace(Constant.TITLE_IDENTIFIER,""));	
				}
				else
				{
					paperscoreBuffer.append(value.toString());
				}

			}
			myBuffer.append(paperscoreBuffer.toString());
			if(!(titleBuffer.toString().isEmpty())){
				myBuffer.append(Double.parseDouble(titleBuffer.toString()));
			}

			context.write(key, new Text((myBuffer.toString())));
		}


	}






}
