package it.pad.pageranker;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import it.pad.PageRankWritable;

public class RankerMapper extends Mapper <LongWritable, Text, Text, PageRankWritable>{

	private PageRankWritable outValue=new PageRankWritable();
	private Text outKey=new Text();

	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		outValue=new PageRankWritable(value);
		//emit the line itself to rebuild the original graph
		outKey.set(outValue.getSource());
		context.write(outKey, outValue);
		//then emit a key-value for each destination node with the destination node id and the pagerank of the analyzed node
		String[] adjacencyArray=outValue.getAdjacencyList().split("\\s");
		outValue.clearAdjacencyList();
		double outgoingPageRank=outValue.getPageRank()/adjacencyArray.length;
		outValue.setPageRank(outgoingPageRank);
		for(String destinationId : adjacencyArray){
			//outValue.setSource(destinationId);
			outKey.set(destinationId);
			context.write(outKey, outValue);
		}
		
	}

}
