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
	private long nodes;
	private double dampingFactor;
	private double correction;
	private double newPageRank;
	private String source;
	private double sum;

/**
	*	retrive damping factor and number of nodes in the graph
	*/
	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		dampingFactor=context.getConfiguration().getFloat(PageRankConstants.DF_KEY, 0.85f);
		nodes=context.getConfiguration().getLong(PageRankConstants.N_KEY, 0);
		correction=(double)(1-dampingFactor)/nodes;
		euclideanNorm=0;
		sum=0;
	}
	
/**
	*	for all vales of a key, sum all the pagerank values, multiply by damping factor and correct the final value before writing
	*/	
	@Override
	public final void reduce(Text inputKey, Iterable<PageRankWritable> inputValues, Context context) throws IOException, InterruptedException{
		source=inputKey.toString();
		newPageRank=0;
		PageRankWritable outValue=null;
		
		//TODO: delet.
		/*
		if(source.compareTo("")==0){
			for(PageRankWritable prw : inputValues){
				prw.setPageRank(-1);
				context.write(NullWritable.get(), prw);
			}
			outValue=new PageRankWritable();
			outValue.setPageRank(-2);
			outValue.setSource(inputKey.toString());
			context.write(NullWritable.get(), outValue);
			return;
		}
		*/
		
		for(PageRankWritable prw : inputValues){
			//this is the original adjacency list, needed to rebuild the graph
			if(!prw.hasEmptySource()){
				outValue=new PageRankWritable(prw);
				continue;
			}
			//this is a term coming from an ingoing edge
			newPageRank+=prw.getPageRank();
		}
		
		newPageRank*=dampingFactor;
		newPageRank+=correction;
		euclideanNorm+=Math.pow((newPageRank-outValue.getPageRank()), 2);
		sum+=newPageRank;
		outValue.setPageRank(newPageRank);
		context.write(NullWritable.get(), outValue);
	}

/**
	* increment the counter to keep track of the euclidean norm
	*/
	@Override
	protected final void cleanup(Context context){
		context.getCounter(PageRankCounters.RANK_NORM).increment((long)euclideanNorm*nodes);
		context.getCounter(PageRankCounters.LOG_VALUE1).increment((long)sum*nodes);
	}
}
