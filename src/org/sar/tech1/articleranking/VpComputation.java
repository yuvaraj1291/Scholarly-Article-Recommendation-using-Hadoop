
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
 * The class computes the venue scores.
 * 
 * Venue score for each venue is an average of all the articles published at that venue or by that publication.
 *
 */

public class VpComputation extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(VpComputation.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new VpComputation(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		Job job = Job.getInstance(config, "VpComputation");
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

			StringBuffer initialBuffer=new StringBuffer();
			StringBuffer valueBuffer=new StringBuffer();

			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			for(String record:recArr){


				String[] currentvalue=record.split("\t");
				String[] valueSplit=currentvalue[1].split(Constant.SEPARATOR);
				String artcileScore=valueSplit[0];
				String venue=valueSplit[2];
				initialBuffer.append(artcileScore);

				String articleId=currentvalue[0];
				initialBuffer.append(Constant.SEPARATOR);
				initialBuffer.append(articleId);
				initialBuffer.append(Constant.SEPARATOR);

				if(valueSplit[1].contains(Constant.AUTHORSEPARATOR)){
					if(!(venue.trim().isEmpty())){
						String authors[]=valueSplit[1].split(Constant.AUTHORSEPARATOR);

						//For many authors
						for(int i=0;i<authors.length;i++)
						{
							valueBuffer.append(initialBuffer);
							if(!(authors[i].trim().isEmpty())){
								valueBuffer.append(authors[i]);
								valueBuffer.append(Constant.SEPARATOR);
								context.write(new Text(venue),new Text(valueBuffer.toString()));
								valueBuffer.setLength(0);
							}					
						}
					}
				}
				else
				{
					//One author
					if(!(venue.trim().isEmpty())){
						valueBuffer.append(initialBuffer);
						if(!(valueSplit[1].trim().isEmpty())){
							valueBuffer.append(valueSplit[1]);
							valueBuffer.append(Constant.SEPARATOR);
							context.write(new Text(venue),new Text(valueBuffer.toString()));
							valueBuffer.setLength(0);
						}

						//Missing author information
						else
						{
							valueBuffer.append(initialBuffer);
							valueBuffer.append(" ");
							valueBuffer.append(Constant.SEPARATOR);
							context.write(new Text(venue),new Text(valueBuffer.toString()));
							valueBuffer.setLength(0);
						}
					}
				}
			}
		}
	}

	public static class Reduce extends	Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {
			double aggregateArticleScore=0.0;
			double Vp=0.0;
			int counter=0;
			StringBuffer valueBuffer=new StringBuffer();
			StringBuffer articleIdBuffer=new StringBuffer();
			StringBuffer authorBuffer=new StringBuffer();

			for(Text value:valueList){

				String[] articleScoreSplit=value.toString().split(Constant.SEPARATOR);
				String currentArticleScore=articleScoreSplit[0];
				String articleId=articleScoreSplit[1];

				String author=articleScoreSplit[2];


				if(!( articleIdBuffer.toString().isEmpty() ))
				{articleIdBuffer.append(Constant.CITATION_IDENTIFIER);}	
				articleIdBuffer.append(articleId);


				if(!(authorBuffer.toString().contains(author))){
					if(!( authorBuffer.toString().isEmpty() ))
					{authorBuffer.append(Constant.AUTHORSEPARATOR);}

					authorBuffer.append(author);}	
				aggregateArticleScore+=Double.parseDouble(currentArticleScore);				
				counter++;
			}


			Vp=aggregateArticleScore/(float)counter;
			valueBuffer.append(Vp);
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(authorBuffer.toString());
			valueBuffer.append(Constant.SEPARATOR);
			valueBuffer.append(articleIdBuffer.toString());
			valueBuffer.append(Constant.SEPARATOR);

			context.write(key, new Text(valueBuffer.toString()));
		}
	}

}



