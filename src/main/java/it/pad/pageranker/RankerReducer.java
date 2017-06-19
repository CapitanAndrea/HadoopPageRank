package it.pad.pageranker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;
import it.pad.PageRankConstants;

public class RankerReducer extends Reducer<Text, PageRankWritable, NullWritable, PageRankWritable>{
	
	private double euclideanNorm;
	private float dampingFactor;
	private float correction;
	private double newPageRank;

/**
	*	retrive damping factor and number of nodes in the graph
	*/
	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		dampingFactor=context.getConfiguration().getFloat(PageRankConstants.DF_KEY, 0.85f);
		correction=context.getConfiguration().getLong(PageRankConstants.N_KEY, 0);
		if(correction>0){
			correction=(1-dampingFactor)/correction;
		}
		euclideanNorm=0;
	}
	
/**
	*	for all vales of a key, sum all the pagerank values, multiply by damping factor and correct the final value before writing
	*/	
	@Override
	public final void reduce(Text inputKey, Iterable<PageRankWritable> inputValues, Context context) throws IOException, InterruptedException{
		newPageRank=0;
		PageRankWritable outValue=null;
		for(PageRankWritable prw : inputValues){
			if(!prw.hasEmptyAdjacencyList()){
				outValue=new PageRankWritable(prw);
				continue;
			}
			newPageRank+=prw.getPageRank();
		}
		newPageRank*=dampingFactor;
		newPageRank+=correction;
		euclideanNorm+=Math.pow((newPageRank-outValue.getPageRank()), 2);
		outValue.setPageRank(newPageRank);
		context.write(NullWritable.get(), outValue);
	}

/**
	* increment the counter to keep track of the euclidean norm
	*/
	@Override
	protected final void cleanup(Context context){
		context.getCounter(PageRankCounters.RANK_NORM).increment(Double.doubleToLongBits(euclideanNorm));
	}
}
