package org.sar.tech1.articleranking;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * This class contains code to compute initial article scores 
 * 
 * The data set is processed and the initial article score is computed based on:
 * 		Indegree, Outdegree, Maximum outdegree and maximum indegree( from all the articles)
 * 		Predefined constants of alpha and gamma are used. 
 *	
 * The class will give the information of inlinks, initial article score, author and venue/publication information.
 *
 *
 *
 */
public class InitialArticleScore  extends Configured implements Tool {

	static final Logger LOG =Logger.getLogger(InitialArticleScore.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);
		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new InitialArticleScore(), args);
	}

	public  int run(String[] args) throws Exception {	

		//We over write the text input format and use it as a record delimiter and every article info starts with #index
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter","#index");

		Job job = Job.getInstance(conf, "InitialArticleScore");
		job.setJarByClass(this.getClass());	

		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}


		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		// Get the maximum indegree and maximum outdegree from all the articles in data set. 
		int maxIndegree=(int) job.getCounters().findCounter("maxIndegree","maxIndegree").getValue();
		int maxOutdegree=(int) job.getCounters().findCounter("maxOutdegree","maxOutdegree").getValue();

		conf.setInt("maxOutdegree", maxOutdegree);
		conf.setInt("maxIndegree", maxIndegree);

		Path output2 = new Path(args[2]);
		// delete existing directory
		if (hdfs.exists(output2)) {
			hdfs.delete(output2, true);
		}

		Job job2 = Job.getInstance(conf, "mr2");
		job2.setJarByClass(this.getClass());	

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.setMapperClass(Map2.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		return job2.waitForCompletion(true)? 0:1;

	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override 
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			int maxOutdegree=(int)context.getCounter("maxOutdegree","maxOutdegree").getValue();
			int OutDegree=0;

			String line=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(line);
			String AuthorList;
			String[] Authors;
			String Venue;
			StringBuffer ValueBuffer=new StringBuffer();
			StringBuffer OutlinkBuffer=new StringBuffer();

			if(!(recArr[0].isEmpty()))
			{
				String value=recArr[0].trim();

				for(String record:recArr){

					//Author information retrieval
					if(record.startsWith(Constant.AUTHOR_IDENTIFIER))
					{
						ValueBuffer.append(Constant.SEPARATOR);
						AuthorList=(record.toString().replace(Constant.AUTHOR_IDENTIFIER,"").trim());
						if(AuthorList.isEmpty())
						{
							ValueBuffer.append(" ");
						}
						else if(AuthorList.trim().contains(";"))
						{
							Authors=AuthorList.trim().split(";");
							for(int i=0;i<Authors.length;i++)
							{
								ValueBuffer.append(Authors[i]);
								if(i!=Authors.length-1){
									ValueBuffer.append(Constant.AUTHORSEPARATOR);}

							}	
						}
						else
						{
							ValueBuffer.append(AuthorList);
						}
						ValueBuffer.append(Constant.SEPARATOR);

					}

					//Venue information retrieval
					if(record.startsWith(Constant.VENUE_IDENTIFIER))
					{


						Venue=(record.toString().replace(Constant.VENUE_IDENTIFIER,"").trim());
						if(Venue.isEmpty())
						{
							ValueBuffer.append(" ");

						}
						else
						{
							ValueBuffer.append(Venue);
						}
						ValueBuffer.append(Constant.SEPARATOR);
					}

					//Citation information retrieval
					if(record.startsWith(Constant.CITATION_IDENTIFIER))
					{							 
						if(OutDegree>0)
						{
							OutlinkBuffer.append("#%");

						}
						int OutLink = Integer.parseInt(record.toString().replace(Constant.CITATION_IDENTIFIER,"").trim());
						OutlinkBuffer.append(OutLink);

						context.write(new IntWritable((OutLink)), new Text("1"));
						OutDegree++;	
					}
				}

				//If no outlinks ---->
				if((OutlinkBuffer.toString().isEmpty()))
				{	

					OutlinkBuffer.append(" ");	
				}

				//Update the maximum outdegree
				if(maxOutdegree<OutDegree)
				{
					context.getCounter("maxOutdegree","maxOutdegree").setValue(OutDegree);
				}
				
				ValueBuffer.append(OutlinkBuffer);
				ValueBuffer.append(Constant.SEPARATOR);
				context.write(new IntWritable(Integer.parseInt(value)), new Text(ValueBuffer.toString()));
			}	
		}

	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text > {	
		@Override 
		public void reduce(IntWritable keyText, Iterable<Text> valueList, Context context) throws IOException, InterruptedException {
			StringBuffer ValueBuffer=new StringBuffer();
		
			int maxIndegree=(int)context.getCounter("maxIndegree","maxIndegree").getValue();
			int inDegree=0;

			for(Text value:valueList){
				if(value.toString().contains(Constant.SEPARATOR))
				{
					ValueBuffer.append(value);
				}
				else
				{
					inDegree+=Integer.parseInt(value.toString());
				}
			}
			
			//Update the maximum indegree
			if(inDegree>maxIndegree)
			{
				context.getCounter("maxIndegree","maxIndegree").setValue(inDegree);
			}
			
			ValueBuffer.append(Constant.INDEGREE);
			ValueBuffer.append(inDegree);
			context.write(keyText,new Text(ValueBuffer.toString()));
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		@Override 
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			StringBuffer valueBuffer=new StringBuffer();
			String data=lineText.toString();
			double alpha= 1.7;
			double beta=3;
			
			// Get the maximum indegree and outdegree of all the articles
			double MaxIndegree=Double.parseDouble(context.getConfiguration().get("maxIndegree"));
			double MaxOutdegree=Double.parseDouble(context.getConfiguration().get("maxOutdegree"));
			String[] recArr=Constant.LINEPATTERN.split(data);
			
			for(String record:recArr){

				String[] currentArticle=record.split("\t");
				String articleId=currentArticle[0];
				String articleInfo=currentArticle[1];

				String[] indegreeSplit=articleInfo.split("INDEGREE");
				int articleIndegree=Integer.parseInt(indegreeSplit[1]);

				String[] outdegreeSplit=articleInfo.split("#%");
				int articleOutdegree=((outdegreeSplit.length)-1);
				
				//initial article score computation
				double Numerator= (alpha * (articleIndegree/MaxIndegree));
				Numerator+= Math.pow((double)articleOutdegree/MaxOutdegree, (double)beta);
				double denominator=1+alpha;
				double article_score=Numerator/denominator;

				valueBuffer.append(article_score);
				valueBuffer.append(indegreeSplit[0]);

				context.write(new IntWritable(Integer.parseInt(articleId)), new Text(valueBuffer.toString()));
				valueBuffer.setLength(0);
			}


		}
	}


}


