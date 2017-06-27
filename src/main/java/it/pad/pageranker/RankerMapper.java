package it.pad.pageranker;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;

public class RankerMapper extends Mapper <LongWritable, Text, Text, PageRankWritable>{

	private PageRankWritable outValue;
	private Text outKey=new Text();

	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		outValue=new PageRankWritable(value);

		//emit the line itself to rebuild the original graph
		outKey.set(outValue.getSource());
		context.write(outKey, outValue);
		
		//then emit a key-value for each destination node with the destination node id and the pagerank of the analyzed node
		outValue.clearSource();
		if(outValue.hasEmptyAdjacencyList()){
		//if a node has no outgoing edges, it votes for itself
			context.write(outKey, outValue);
		} else{
			String[] adjacencyArray=outValue.getAdjacencyList().split("\\t");
			double outgoingPageRank=outValue.getPageRank()/adjacencyArray.length;
			outValue.setPageRank(outgoingPageRank);
			outValue.clearAdjacencyList();
			for(String destinationId : adjacencyArray){
				outKey.set(destinationId);
				context.write(outKey, outValue);
			}
		}
	}

}
