package it.pad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.TreeSet;

import it.pad.parser.AdjacencyListMapper;
import it.pad.parser.ParserReducer;
import it.pad.pageranker.RankerMapper;
import it.pad.pageranker.RankerReducer;
import it.pad.sorter.SorterMapper;
import it.pad.sorter.DescendingRankSorterReducer;
import it.pad.PageRankConstants;

//TODO: vedrai mi tocca far specificare come argomento il numero di nodi del grafo! è ragionevole, ma bleah =\

public class PageRankDriver{

	public static void main(String[] args) throws Exception{
		float dampingFactor=0.85f;
		double error=0;
		int iterations=0;
		int maxIterations=10;
		long nodes=0;
		int result_nodes=0;
		
		/*	arguments parsing	*/
		
		/*	parsing job	*/
		Job parsingJob=new Job(new Configuration(), "parsing_job");
		parsingJob.setJarByClass(PageRankDriver.class);

//	TODO: aggiungere la possiblità di scegliere il parser?
		parsingJob.setMapperClass(AdjacencyListMapper.class);
		parsingJob.setReducerClass(ParserReducer.class);
		
		parsingJob.setMapOutputKeyClass(Text.class);
		parsingJob.setMapOutputValueClass(Text.class);
		
		parsingJob.setOutputKeyClass(NullWritable.class);
		parsingJob.setOutputValueClass(PageRankWritable.class);
		
		FileInputFormat.setInputPaths(parsingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(parsingJob, new Path("pr_0"));
		
		//parsingJob.getConfiguration().setFloat(PageRankConstants.DF_KEY, dampingFactor);
		parsingJob.waitForCompletion(true);
		nodes=parsingJob.getCounters().findCounter(PageRankCounters.NODES_COUNT).getValue();
		
		/*	page rank computation	*/
		
		Configuration configuration=new Configuration();
		configuration.setFloat(PageRankConstants.DF_KEY, dampingFactor);
		configuration.setLong(PageRankConstants.N_KEY, nodes);
		
		FileSystem fs=FileSystem.get(configuration);
		
		for(iterations=1; iterations<=maxIterations; iterations++){
		
			Job rankingJob=new Job(configuration, "ranking_job");
			rankingJob.setJarByClass(PageRankDriver.class);
	
			rankingJob.setMapperClass(RankerMapper.class);
			rankingJob.setReducerClass(RankerReducer.class);
	
			rankingJob.setMapOutputKeyClass(Text.class);
			rankingJob.setMapOutputValueClass(PageRankWritable.class);
	
			rankingJob.setOutputKeyClass(NullWritable.class);
			rankingJob.setOutputValueClass(PageRankWritable.class);
			
			Path input=new Path("pr_" + (iterations-1));
			FileInputFormat.setInputPaths(rankingJob, input);
			FileOutputFormat.setOutputPath(rankingJob, new Path("pr_" + iterations));
		
			rankingJob.waitForCompletion(true);
			System.out.println("\t\tITERATION " + iterations + " COMPLETED. THE 2-NORM OF THE STEP IS: " + Math.sqrt(Double.longBitsToDouble(rankingJob.getCounters().findCounter(PageRankCounters.RANK_NORM).getValue())));
			
			fs.delete(input, true);
			
		}
		
		/*	final sort	*/
		
		Job sortingJob=new Job(new Configuration(), "sorting_job");
		sortingJob.setJarByClass(PageRankDriver.class);
		
		sortingJob.setMapperClass(SorterMapper.class);
//	TODO: aggiungere la possibilità di scegliere il modo di sortare gli elementi?
		sortingJob.setReducerClass(DescendingRankSorterReducer.class);
		
		sortingJob.setMapOutputKeyClass(NullWritable.class);
		sortingJob.setMapOutputValueClass(PageRankWritable.class);
		
		sortingJob.setOutputKeyClass(NullWritable.class);
		sortingJob.setOutputValueClass(PageRankWritable.class);
		
		sortingJob.getConfiguration().setLong(PageRankConstants.RES_KEY, nodes);
		
		Path input=new Path("pr_" + (iterations-1));
		FileInputFormat.setInputPaths(sortingJob, input);
		FileOutputFormat.setOutputPath(sortingJob, new Path("result"));
		
		sortingJob.waitForCompletion(true);
		
		fs.delete(input, true);

	}
}
