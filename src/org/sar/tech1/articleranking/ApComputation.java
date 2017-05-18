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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Author score based on each paper is computed in this class..
 * 
 * Author score along with the list of papers the author published will be returned from this class.
 *
 */

public class ApComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(ApComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new ApComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing output directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		Job job = Job.getInstance(config, "ApComputation");
		job.setJarByClass(this.getClass());

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]) );

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

			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);

			for(String record:recArr){
				String[] valueSplit=record.split(Constant.TABSEPERATOR);
				String authorSplit[]=valueSplit[1].split(Constant.SEPARATOR);
				String authors=authorSplit[1];
				String articleId=valueSplit[0];
				String articleScore=authorSplit[0];

				valueBuffer.append(articleId);
				valueBuffer.append(Constant.SEPARATOR);
				valueBuffer.append(articleScore);

				if(authors.contains(Constant.AUTHORSEPARATOR))
				{
					String currentAuthor[]=authors.split(Constant.AUTHORSEPARATOR);

					//For mutliple authors in an article
					for(int i=0;i<currentAuthor.length;i++)	
					{
						if(!(currentAuthor[i].trim().isEmpty()))
						{	
							context.write(new Text(currentAuthor[i].trim()), new Text(valueBuffer.toString()) );
						}
					}
				}

				//For single author articles
				else if(!(authors.trim().isEmpty()))
				{
					context.write(new Text(authors.trim()), new Text(valueBuffer.toString()) );
				}

				//For missing author information articles
				else
				{
					continue;
				}
			}
		}
	}

	public static class Reduce extends
	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregateArticleScore=0;
			double ap=0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer articleIdBuffer=new StringBuffer();

			for(Text value:valueList){

				String[] articleScoreSplit=value.toString().split(Constant.SEPARATOR);
				String currentArticleScore=articleScoreSplit[1];
				String articleId=articleScoreSplit[0];
				articleIdBuffer.append(articleId);

				//Seperator is added for more than one papers an author wrote
				if(!( articleIdBuffer.toString().isEmpty() ))
				{articleIdBuffer.append(Constant.CITATION_IDENTIFIER);}	

				aggregateArticleScore+=Double.parseDouble(currentArticleScore.toString());				
				counter++;
			}

			ap=aggregateArticleScore/(float)counter;
			valueBuffer.append(ap);
			valueBuffer.append(Constant.SEPARATOR);

			//Also add the papers written by author to output
			valueBuffer.append(articleIdBuffer.toString());
			valueBuffer.append(Constant.SEPARATOR);
			context.write(key, new Text(valueBuffer.toString()));
		}
	}

}


