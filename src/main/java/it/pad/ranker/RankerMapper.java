package it.pad.ranker;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import it.pad.PageRankWritable;

public class RankerMapper extends Mapper <LongWritable, Text, Text, PageRankWritable>{

	private PageRankWritable outValue;
	private Text outKey=new Text();

	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		outValue=new PageRankWritable(value);

		//Emit the line itself to rebuild the original graph
		outKey.set(outValue.getSource());
		context.write(outKey, outValue);
		//Then emit a pair for each destination node with the destination node id as key and the pagerank of the analyzed node
		//clear the source so that it's easy for the reducer to distinguish the values used to rebuild the graph from the values used to compute a new approximation of PageRank
		outValue.clearSource();
		if(outValue.hasEmptyAdjacencyList()){
		//If a node has no outgoing edges, it votes for itself. This preserves the fact that the measure is a probability ditribution
			context.write(outKey, outValue);
		} else{
			String[] adjacencyArray=outValue.getAdjacencyList().split("\\t");
			double outgoingPageRank=outValue.getPageRank()/adjacencyArray.length;
			outValue.setPageRank(outgoingPageRank);
			outValue.clearAdjacencyList(); //clear the adjacency list to reduce the amout of data written
			for(String destinationId : adjacencyArray){
				outKey.set(destinationId);
				context.write(outKey, outValue);
			}
		}
	}

}
