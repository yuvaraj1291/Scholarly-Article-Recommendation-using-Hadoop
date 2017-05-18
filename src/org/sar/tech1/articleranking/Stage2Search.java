package org.sar.tech1.articleranking;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Logger;


//import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stage2Search extends Configured implements Tool {

	static final Logger LOG = Logger.getLogger(Stage2Search.class.getName());
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration(true);

		System.out.println("Give your search string..!");
		@SuppressWarnings("resource")
		Scanner sc=new Scanner(System.in);

		String searchTitle=sc.nextLine();
		conf.set("searchTitle", searchTitle );

		@SuppressWarnings("unused")
		int res = ToolRunner.run(conf,new Stage2Search(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(config);

		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		config.set("fileName",args[0]);


		Job job = Job.getInstance(config, "Stage2Search");
		job.setJarByClass(this.getClass());

		job.setMapperClass(Map2.class);
		job.setSortComparatorClass(sort_comparater.class);
		job.setReducerClass(Reduce2.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));

		TextOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map2 extends
	Mapper<LongWritable, Text, FloatWritable, Text> {

		Set<Integer> set = new HashSet<Integer>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//LOG.info("hereee11111");
			super.setup(context);

			Configuration conf = context.getConfiguration();
			String SearchTitle=conf.get("searchTitle");

			String fileName=conf.get("fileName")+"/part-r-00000";


			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));

			String line;

			outerloop:{
				while ((line=br.readLine()) != null){
					String valueSplit[]=line.split("\t");
					String titleSplit[]=valueSplit[1].split(Constant.SEPARATOR);
					String current_title=titleSplit[0];

					if(current_title.equalsIgnoreCase(SearchTitle))
					{int i=0;
					if(!(titleSplit[1].trim().isEmpty()))
					{

						if((titleSplit[1].contains(Constant.CITATION_IDENTIFIER)))
						{ 
							String outlinkList[]=titleSplit[1].split(Constant.CITATION_IDENTIFIER);
							for( i=0;i<outlinkList.length;i++)
							{
								set.add(Integer.parseInt(outlinkList[i].trim()));	
								if(i==(outlinkList.length-1))
								{
									break outerloop;	
								}
							}


						}


						else
						{
							set.add(Integer.parseInt(titleSplit[1]));
							break outerloop;
						}

					}
					}
				}
				br.close();
			}
		}


		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String data=lineText.toString();
			String[] recArr=Constant.LINEPATTERN.split(data);
			for(String record:recArr){
				String record3[]=record.split("\t");
				if(set.contains(Integer.parseInt(record3[0])))
				{
					String[] currenttitleSplit=record3[1].split(Constant.SEPARATOR);
					String currentTitle=currenttitleSplit[0];
					double currentScore=Double.parseDouble((currenttitleSplit[2]));
					context.write(new FloatWritable((float)currentScore),new Text(currentTitle) );
				}
			}

		}

	}

	public static class Reduce2 extends
	Reducer<FloatWritable, Text,Text , FloatWritable> {
		@Override
		public void reduce(FloatWritable key, Iterable<Text> valueList,Context context) throws IOException, InterruptedException {

			for(Text value:valueList){
				context.write(value, key);
			}
		}


	}

	// We sort the page rank values in descending order using sort_comparater.
	public static class sort_comparater extends WritableComparator {
		protected sort_comparater(){
			super(FloatWritable.class,true);
		}
		@SuppressWarnings("rawtypes")
		@Override 
		public int compare( WritableComparable compositeKey1, WritableComparable compositeKey2) {
			FloatWritable key1=(FloatWritable)(compositeKey1);
			FloatWritable key2=(FloatWritable)(compositeKey2);
			return -1*key1.compareTo(key2);
		}

	}
}
