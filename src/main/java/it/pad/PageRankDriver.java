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

import it.pad.parser.ParserMapper;
import it.pad.parser.ParserReducer;
import it.pad.ranker.RankerMapper;
import it.pad.ranker.RankerReducer;
import it.pad.sorter.SorterMapper;
import it.pad.sorter.SorterReducer;
import it.pad.sorter.PageRankWritableGroupingComparator;
import it.pad.sorter.PageRankWritableKeyComparator;
import it.pad.PageRankConstants;

/**
	* usage: -p parserClassName -s sorterClassName -n numberOfNodes -i inputFile -o outputFile [-m maxIterations] [-d dampingFactor] [-r numReducers] [-t numberOfRecordsToEmit]
	* use split -d -n r/3 source destination to split by number of output files
	* or split -d -C 64M source destination to split by size
*/
public class PageRankDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{
	/*	necessary arguments	*/
		long nodes=-1;
		Class<? extends ParserMapper> parserClass=null;
		Class<? extends PageRankWritableKeyComparator> sorterClass=null;
		String inputFile=null;
		String outputFile=null;
	/*	optional arguments with default values	*/
		float dampingFactor=0.85f;
		long maxIterations=10;
		long emit=-1;
		int numReducers=1;
	/*	computation variables	*/
		int iterations=0;

		/*	arguments parsing	*/
		if(args.length%2!=0) return -1;
		for(int i=0; i<args.length; i++){
			//	parser class
			if(args[i].compareTo("-p")==0){
				i++;
				parserClass=(Class<? extends ParserMapper>)Class.forName(args[i]);
				continue;
			}
			//	sorter class
			if(args[i].compareTo("-s")==0){
				i++;
				sorterClass=(Class<? extends PageRankWritableKeyComparator>)Class.forName(args[i]);
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
				maxIterations=Long.parseLong(args[i]);
				continue;
			}
			//	number of top values to emit
			
			if(args[i].compareTo("-t")==0){
				i++;
				emit=Long.parseLong(args[i]);
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
		
		/*check if the arguments passed are valid*/
		if(nodes<1){
			System.out.println("|=ERROR=| The number of nodes was not specified or less than 1");
			return -1;
		}
		if(parserClass==null || sorterClass==null || inputFile==null || outputFile==null){
			System.out.println("|=ERROR=| Requested arguments: -p parserClassName -s sorterClassName -n numberOfNodes -i inputFile -o outputFile"); 
			System.out.println("|=ERROR=| Optional arguments: [-m maxIterations] [-d dampingFactor] [-r numReducers] [-t numberOfRecordsToEmit]");
			return -1;
		}
		if(!ParserMapper.class.isAssignableFrom(parserClass)){
			System.out.println("|=WARNING=| It is STRONGLY recommended that your parser class extends it.pad.parser.ParserMapper");
		}
		if(!PageRankWritableKeyComparator.class.isAssignableFrom(sorterClass)){
			System.out.println("|=WARNING=| It is STRONGLY recommended that your sorter class extends it.pad.sorter.PageRankWritableKeyComparator");
		}
		if(emit==-1) emit=nodes;

		/*	parsing job	*/
		System.out.println("|=LOG=|\t\tSTARTING PARSING JOB.");
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
		System.out.println("|=LOG=|\t\tPARSING JOB COMPLETED.");
		

		/*	page rank computation	*/
		configuration=new Configuration();
		configuration.setFloat(PageRankConstants.DF_KEY, dampingFactor);
		configuration.setLong(PageRankConstants.N_KEY, nodes);

		FileSystem fs=FileSystem.get(configuration);

		for(iterations=1; iterations<=maxIterations; iterations++){
			System.out.println("|=LOG=|\t\tSTARTING ITERATION " + iterations + ".");

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
			System.out.println("|=LOG=|\t\tITERATION " + iterations + " COMPLETED.");
			
			//delete the files no more needed
			fs.delete(input, true);
		}

		/*	final sort	*/
		System.out.println("|=LOG=|\t\tSTARTING SORING JOB.");
		Job sortingJob=new Job(new Configuration(), "sorting_job");
		sortingJob.setJarByClass(PageRankDriver.class);

		sortingJob.setMapperClass(SorterMapper.class);
		sortingJob.setReducerClass(SorterReducer.class);

		sortingJob.setMapOutputKeyClass(PageRankWritable.class);
		sortingJob.setMapOutputValueClass(PageRankWritable.class);
		
		sortingJob.setOutputKeyClass(NullWritable.class);
		sortingJob.setOutputValueClass(PageRankWritable.class);

		//this grouping comparator is used to send all the pairs to one reducer
		sortingJob.setGroupingComparatorClass(PageRankWritableGroupingComparator.class);
		//this sort comparator is used to perform secondary sorting
		sortingJob.setSortComparatorClass(sorterClass);

		//set the total number of records to emit
		sortingJob.getConfiguration().setLong(PageRankConstants.RES_KEY, emit);
		sortingJob.setNumReduceTasks(1);

		Path input=new Path("pr_" + (iterations-1));
		FileInputFormat.setInputPaths(sortingJob, input);
		FileOutputFormat.setOutputPath(sortingJob, new Path(outputFile));

		sortingJob.waitForCompletion(true);

		fs.delete(input, true);
		System.out.println("|=LOG=|\t\tSORTING JOB COMPLETED.");
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int result=new PageRankDriver().run(args);
		System.exit(result);
	}
}
