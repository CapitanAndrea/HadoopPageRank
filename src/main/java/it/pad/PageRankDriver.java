package it.pad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import it.pad.parser.AdjacencyListMapper;
import it.pad.parser.EdgeListMapper;
import it.pad.parser.ParserReducer;
import it.pad.pageranker.RankerMapper;
import it.pad.pageranker.RankerReducer;
import it.pad.sorter.SorterMapper;
import it.pad.sorter.DescendingRankSorterReducer;
import it.pad.PageRankConstants;

//TODO: la somma non fa 1, edit: ora sì, ma forse va bene anche se non fa 1
//TODO: aggiungere test
//TODO: ricontrollare codice
//TODO: pulire codice

/**
	* usage: -p parserClassName -s sorterClassName -n numberOfNodes -i inputFile -o outputFile [-m maxIterations] [-e errorThreshold] [-d dampingFactor] [-r numReducers]
	* use split -d -n r/3 data/pr-xxsmall.txt google-splits/ to split by number of output files
	* or split -d -C 64M data/web-Google.txt google-splits/ to split by size
*/
public class PageRankDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{
	/*	necessary arguments	*/
		long nodes=-1;
		Class parserClass=null;
		Class sorterClass=null;
		String inputFile=null;
		String outputFile=null;
	/*	optional arguments with default values	*/
		float dampingFactor=0.85f;
		double errorThreshold=0;
		int maxIterations=10;
		int numReducers=1;
	/*	computation variables	*/
		int iterations=0;
		double error=0;
		int result_nodes=0;

		/*	arguments parsing	*/
		if(args.length%2!=0) return -1;
		for(int i=0; i<args.length; i++){
			//	parser class
			if(args[i].compareTo("-p")==0){
				i++;
				parserClass=Class.forName(args[i]);
				continue;
			}
			//	sorter class
			if(args[i].compareTo("-s")==0){
				i++;
				sorterClass=Class.forName(args[i]);
				continue;
			}
			//	number of nodes in the input graph
			if(args[i].compareTo("-n")==0){
				i++;
				nodes=Long.parseLong(args[i]);
				continue;
			}
			//	path of the input file
			if(args[i].compareTo("-i")==0){
				i++;
				inputFile=args[i];
				continue;
			}
			//	path of the output file
			if(args[i].compareTo("-o")==0){
				i++;
				outputFile=args[i];
				continue;
			}
			//	max number of iterations of the approximation
			if(args[i].compareTo("-m")==0){
				i++;
				maxIterations=Integer.parseInt(args[i]);
				continue;
			}
			//	error threshold to reach before stopping
			if(args[i].compareTo("-e")==0){
				i++;
				errorThreshold=Double.parseDouble(args[i]);
				continue;
			}
			//	damping factor to be used
			if(args[i].compareTo("-d")==0){
				i++;
				dampingFactor=Float.parseFloat(args[i]);
				continue;
			}
			//	number of reducers to be used
			if(args[i].compareTo("-r")==0){
				i++;
				numReducers=Integer.parseInt(args[i]);
				continue;
			}
		}
		if(nodes<1 || parserClass==null || sorterClass==null || inputFile==null || outputFile==null) return -1;

		/*	parsing job	*/
		Configuration configuration=new Configuration();
		configuration.setLong(PageRankConstants.N_KEY, nodes);
		
		Job parsingJob=new Job(configuration, "parsing_job");
		parsingJob.setJarByClass(PageRankDriver.class);

		parsingJob.setMapperClass(parserClass);
		parsingJob.setReducerClass(ParserReducer.class);

		parsingJob.setMapOutputKeyClass(Text.class);
		parsingJob.setMapOutputValueClass(Text.class);

		parsingJob.setOutputKeyClass(NullWritable.class);
		parsingJob.setOutputValueClass(PageRankWritable.class);

		FileInputFormat.setInputPaths(parsingJob, new Path(inputFile));
		FileOutputFormat.setOutputPath(parsingJob, new Path("pr_0"));

		parsingJob.setNumReduceTasks(numReducers);
		parsingJob.waitForCompletion(true);
		System.out.println("\t\t The sum of all pr values in reducer is: " + Double.longBitsToDouble(parsingJob.getCounters().findCounter(PageRankCounters.LOG_VALUE1).getValue()));
		

		/*	page rank computation	*/
		configuration=new Configuration(); //è veramente necessario un nuovo oggetto?
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

			rankingJob.setNumReduceTasks(numReducers);

			rankingJob.waitForCompletion(true);
			double norm=Math.sqrt(Double.longBitsToDouble(rankingJob.getCounters().findCounter(PageRankCounters.RANK_NORM).getValue()));
			System.out.println("\t\tITERATION " + iterations + " COMPLETED. THE 2-NORM OF THE STEP IS: " + norm);
			//System.out.println("\t\t The sum of all pr values in mapper is: " + Double.longBitsToDouble(rankingJob.getCounters().findCounter(PageRankCounters.LOG_VALUE2).getValue()));
			System.out.println("\t\t The sum of all pr values in reducer is: " + Double.longBitsToDouble(rankingJob.getCounters().findCounter(PageRankCounters.LOG_VALUE1).getValue()));

			if(norm<=errorThreshold) break;
			fs.delete(input, true);

		}

		/*	final sort	*/
		Job sortingJob=new Job(new Configuration(), "sorting_job");
		sortingJob.setJarByClass(PageRankDriver.class);

		sortingJob.setMapperClass(SorterMapper.class);
		sortingJob.setReducerClass(sorterClass);

		sortingJob.setMapOutputKeyClass(NullWritable.class);
		sortingJob.setMapOutputValueClass(PageRankWritable.class);

		sortingJob.setOutputKeyClass(NullWritable.class);
		sortingJob.setOutputValueClass(PageRankWritable.class);

		sortingJob.getConfiguration().setLong(PageRankConstants.RES_KEY, nodes);

		Path input=new Path("pr_" + (iterations-1));
		FileInputFormat.setInputPaths(sortingJob, input);
		FileOutputFormat.setOutputPath(sortingJob, new Path(outputFile));

		sortingJob.waitForCompletion(true);

		fs.delete(input, true);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int result=new PageRankDriver().run(args);
	}
}
