package org.sar.tech1.articleranking;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * Author scores are recomputed based on venues.
 * 
 * The scores formed here are taken for author score refinement
 *
 */

public class AvComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(AvComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new AvComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "AvComputation");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class Map extends
	Mapper<LongWritable, Text, Text, FloatWritable> {

		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){
				String[] valueSplit=record.split("\t");
				String outlinkSplit[]=valueSplit[1].split(Constant.SEPARATOR);
				String Vp=(outlinkSplit[0]);
				if(outlinkSplit.length>1)
				{
					String authors=outlinkSplit[1];

					//One or many authors
					if(!((authors.trim()).isEmpty()))	
					{
						if(authors.contains(Constant.AUTHORSEPARATOR)){
							String currentAuthors[]=authors.split(Constant.AUTHORSEPARATOR);
							for(int i=0;i<currentAuthors.length;i++)
							{
								context.write(new Text((currentAuthors[i].trim())), new FloatWritable(Float.parseFloat(Vp)));	
							}
						}
						else
						{
							context.write(new Text((authors.trim())), new FloatWritable(Float.parseFloat(Vp)));
						}
					}

				}

			}
		}
	}


	public static class Reduce extends
	Reducer<Text, FloatWritable, Text, FloatWritable> {
		@Override
		public void reduce(Text key, Iterable<FloatWritable> valueList,Context context) throws IOException, InterruptedException {
			double aggregateAuthorScore=0.0;
			double av=0;
			int counter=0;

			for(FloatWritable value:valueList){
				aggregateAuthorScore+=Double.parseDouble(value.toString());				
				counter++;
			}
			av=aggregateAuthorScore/(float)counter;
			context.write(key, new FloatWritable((float)(av)));
		}
	}

}


