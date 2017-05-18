package org.sar.tech1.articleranking;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Author score refinement is computed in this class
 * 
 * Refined author score is average of Av and Ar.
 *
 */
public class ArComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(ArComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new ArComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Job job = Job.getInstance(config, "ArComputation");
		job.setJarByClass(this.getClass());
		Path output = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
	Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){
				String[] authorSplit=record.split(Constant.TABSEPERATOR);
				context.write(new Text(authorSplit[0]),new Text(authorSplit[1]));
			}
		}
	}

	public static class Reduce extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregateAuthorScore=0.0;
			double ar=0.0;

			StringBuffer articleIdBuffer=new StringBuffer();
			StringBuffer valueBuffer=new StringBuffer();

			for(Text value:valueList){
				if(value.toString().contains(Constant.SEPARATOR))
				{
					String apscoreSplit[]=value.toString().split(Constant.SEPARATOR);
					aggregateAuthorScore+=Double.parseDouble(apscoreSplit[0]);	
					articleIdBuffer.append((apscoreSplit[1]));	
				}
				else
				{
					aggregateAuthorScore+=Double.parseDouble(value.toString());	
				}
			}
			ar=aggregateAuthorScore/(float)2;
			valueBuffer.append(ar);
			valueBuffer.append(Constant.SEPARATOR);

			if(!(articleIdBuffer.toString().isEmpty()))	
			{
				valueBuffer.append(articleIdBuffer.toString());}

			//Empty article id buffer
			else
			{
				valueBuffer.append(" ");
			}
			valueBuffer.append(Constant.SEPARATOR);

			context.write(key, new Text((valueBuffer.toString())));
		}
	}

}


