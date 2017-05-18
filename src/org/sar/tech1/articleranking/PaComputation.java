package org.sar.tech1.articleranking;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.logging.Logger;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Article score is computed based on author.
 * 
 * The refined author scores are used for this computation.
 *
 */

public class PaComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(PaComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new PaComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		Job job = Job.getInstance(config, "PaComputation");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

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

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){
				String[] valueSplit=record.split("\t");
				String ArSplit[]=valueSplit[1].split(Constant.SEPARATOR);
				BigDecimal Arvalue= new BigDecimal(ArSplit[0]);			

				if(!(ArSplit[1].isEmpty())){
					String articleIdList=ArSplit[1];
					
					//Many articles
					if(articleIdList.contains(Constant.CITATION_IDENTIFIER))
					{
						String pageIds[]=articleIdList.split(Constant.CITATION_IDENTIFIER);
						for(int i=0;i<pageIds.length;i++)
						{
							context.write(new IntWritable(Integer.parseInt(pageIds[i].trim())),new Text((Arvalue.toString())));
						}
					}
					
					//Single article
					else if(!(articleIdList.trim().isEmpty()))
					{
						context.write(new IntWritable(Integer.parseInt(articleIdList.trim())),new Text((Arvalue.toString())));
					}
				}

			}
		}
	}

	public static class Reduce extends
	Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregateArticleScore=0.0;
			double pa=0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();

			for(Text value:valueList){

				aggregateArticleScore+=Double.parseDouble(value.toString());	
				counter++;
			}
			pa=aggregateArticleScore/(float)counter;
			valueBuffer.append(pa);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(Constant.PA_IDENTIFIER);

			context.write(key, new Text(valueBuffer.toString()));
		}
	}

}


