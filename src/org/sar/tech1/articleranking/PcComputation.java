package org.sar.tech1.articleranking;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 
 * Article score based on its citations is computed here...
 * 
 * It is the average of all the article scores an article cites 
 * 
 */

public class PcComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(PcComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new PcComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		
		Job job = Job.getInstance(config, "PcComputation");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
	Mapper<LongWritable, Text, IntWritable, FloatWritable> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){

				String[] valueSplit=record.split("\t");
				String outlinkSplit[]=valueSplit[1].split(Constant.SEPARATOR);
				String articleScore=(outlinkSplit[0]);
				String outlinkList=outlinkSplit[3];
				
				if(!((outlinkList.trim()).isEmpty()))	
				{
					if(outlinkList.contains(Constant.CITATION_IDENTIFIER)){
						String outLinks[]=outlinkList.split(Constant.CITATION_IDENTIFIER);
						for(int i=0;i<outLinks.length;i++)
						{
							context.write(new IntWritable(Integer.parseInt(outLinks[i])), new FloatWritable(Float.parseFloat(articleScore)));	
						}	
					}
				}

				else if(!(outlinkList.trim().contains(Constant.CITATION_IDENTIFIER)))	
				{
					if(!((outlinkList.trim()).isEmpty()))	

					{
						context.write(new IntWritable(Integer.parseInt(outlinkList.trim())), new FloatWritable(Float.parseFloat(articleScore)));
					}
				}
			}
		}
	}

	
	public static class Reduce extends
	Reducer<IntWritable, FloatWritable, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<FloatWritable> valueList,Context context) throws IOException, InterruptedException {
			double aggregateArticleScore=0.0;
			double pc=0.0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();
			for(FloatWritable value:valueList){
				aggregateArticleScore+=Double.parseDouble(value.toString());				
				counter++;
			}
			
			pc=aggregateArticleScore/(float)counter;
			valueBuffer.append(pc);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(Constant.PC_IDENTIFIER);

			context.write(key, new Text(valueBuffer.toString()));

		}
	}

}


