package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;
import it.pad.PageRankConstants;

public class ParserReducer extends Reducer<Text, Text, NullWritable, PageRankWritable>{

	private int nodes=0;
	private static final String separator="\t";
	private PageRankWritable output=new PageRankWritable();
	
	@Override
	protected final void setup(Context context){
		output.setPageRank(0);
	}

	@Override
	public final void reduce(Text source, Iterable<Text> destinations, Context context) throws IOException, InterruptedException{
		
		StringBuilder builder=new StringBuilder();
		for(Text destination : destinations){
			builder.append(destination.toString());
			builder.append(separator);
		}
		
		output.setSource(source.toString());
		output.setAdjacencyList(builder.toString());
		context.write(NullWritable.get(), output);
		
		nodes++;
	}
	
	@Override
	protected final void cleanup(Context context){
		context.getCounter(PageRankCounters.NODES_COUNT).increment(nodes);
	}
}
