package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;
import it.pad.PageRankConstants;

public class ParserReducer extends Reducer<Text, Text, NullWritable, PageRankWritable>{

	private static final String separator="\t";
	private PageRankWritable output=new PageRankWritable();

	@Override
	protected final void setup(Context context){
		// get the total number of nodes to set the starting value of pagerank for all the nodes
		long nodes=context.getConfiguration().getLong(PageRankConstants.N_KEY, 0);
		output.setPageRank((double)1/nodes);
	}

	@Override
	public final void reduce(Text source, Iterable<Text> destinations, Context context) throws IOException, InterruptedException{
		//build the adjacency list
		StringBuilder builder=new StringBuilder();
		String destinationString;
		for(Text destination : destinations){
			destinationString=destination.toString();
			if(destinationString.isEmpty()) continue;
			builder.append(destinationString).append(separator);
		}
		//emit a PRW for each node containing the source node, its adjacency list and the starting value of page rank
		output.setSource(source.toString());
		output.setAdjacencyList(builder.toString().trim());
		context.write(NullWritable.get(), output);
	}
}
