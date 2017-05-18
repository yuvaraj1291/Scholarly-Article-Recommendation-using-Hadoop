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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Using the pa, pv, pc and pi values, a new Article score is computed with gama constant.
 * 
 * This article score is again sent to the next passes for four to five iterations
 *
 */

public class ArticleScoreComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(ArticleScoreComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new ArticleScoreComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = Job.getInstance(config, "ArticleScoreComputation");
		job.setJarByClass(this.getClass());

		Path output = new Path(args[4]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,Map.class);
		TextOutputFormat.setOutputPath(job, new Path(args[4]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
	Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			for(String record:recArr){
				String[] valueSplit=record.split("\t");

				if(StringUtils.countMatches(valueSplit[1], Constant.SEPARATOR)>1)
				{
					String articleScoreSplit[]=valueSplit[1].split(Constant.SEPARATOR);
					valueBuffer.append(articleScoreSplit[0]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(Constant.PI_IDENTIFIER);
					context.write(new IntWritable(Integer.parseInt(valueSplit[0])), new Text(valueBuffer.toString()));
					valueBuffer.setLength(0);
					valueBuffer.append(articleScoreSplit[1]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(articleScoreSplit[2]);
					valueBuffer.append(Constant.SEPARATOR);
					valueBuffer.append(articleScoreSplit[3]);
					valueBuffer.append(Constant.SEPARATOR);
					context.write(new IntWritable(Integer.parseInt(valueSplit[0])), new Text(valueBuffer.toString()));
				}
				else
				{
					context.write(new IntWritable(Integer.parseInt(valueSplit[0])), new Text((valueSplit[1])));
				}	


			}

		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			float gama=(float) 0.3;
			float articleScore=0;
			float pi=0;
			float pa=0;
			float pv = 0;
			float pc=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer currentBuffer=new StringBuffer();

			for(Text value:valueList){
				String currentValue=value.toString();
				if(StringUtils.countMatches(currentValue, Constant.SEPARATOR)>1)
				{
					valueBuffer.append(currentValue);
				}
				else if(currentValue.contains(Constant.SEPARATOR+Constant.PI_IDENTIFIER))
				{
					pi=Float.parseFloat((currentValue.split(Constant.SEPARATOR+Constant.PI_IDENTIFIER))[0]);
				}
				else if(currentValue.contains(Constant.SEPARATOR+Constant.PA_IDENTIFIER))
				{

					pa=Float.parseFloat((currentValue.split(Constant.SEPARATOR+Constant.PA_IDENTIFIER))[0]);
				}
				else if(currentValue.contains(Constant.SEPARATOR+Constant.PV_IDENTIFIER))
				{
					pv=Float.parseFloat((currentValue.split(Constant.SEPARATOR+Constant.PV_IDENTIFIER))[0]);
				}
				else if(currentValue.contains(Constant.SEPARATOR+Constant.PC_IDENTIFIER))
				{
					pc=Float.parseFloat((currentValue.split(Constant.SEPARATOR+Constant.PC_IDENTIFIER))[0]);
				}
				else
				{
					break;
				}


			}			
			float aggregate=pa+pv+pc/3;
			articleScore= ( gama * pi ) + (( 1 - gama ) * aggregate);
			currentBuffer.append(articleScore);
			currentBuffer.append(Constant.SEPARATOR);

			currentBuffer.append(valueBuffer.toString());

			context.write(key,new Text(currentBuffer.toString()));

		}
	}
}




