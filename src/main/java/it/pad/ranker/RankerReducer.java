package it.pad.ranker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import it.pad.PageRankWritable;
import it.pad.PageRankConstants;

public class RankerReducer extends Reducer<Text, PageRankWritable, NullWritable, PageRankWritable>{
	
	private double dampingFactor;
	private double constantTerm;
	private double newPageRank;

	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		//Compute the constant term of the formula using the arguments passed from the driver
		dampingFactor=context.getConfiguration().getFloat(PageRankConstants.DF_KEY, 0.85f);
		long nodes=context.getConfiguration().getLong(PageRankConstants.N_KEY, 0);
		constantTerm=(double)(1-dampingFactor)/nodes;
	}
	
	@Override
	public final void reduce(Text inputKey, Iterable<PageRankWritable> inputValues, Context context) throws IOException, InterruptedException{
		String source=inputKey.toString();
		newPageRank=0;
		PageRankWritable outValue=null;
		
		for(PageRankWritable prw : inputValues){
			//this is the original adjacency list, needed to rebuild the graph
			if(!prw.hasEmptySource()){
				outValue=new PageRankWritable(prw);
				continue;
			}
			//this is a term coming from an ingoing edge
			newPageRank+=prw.getPageRank();
		}
		//compute the new approximation
		newPageRank*=dampingFactor;
		newPageRank+=constantTerm;
		outValue.setPageRank(newPageRank);
		//write the new approximation with the graph on the context
		context.write(NullWritable.get(), outValue);
	}
}
