package it.pad.sorter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Comparator;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;
import it.pad.PageRankConstants;

public abstract class SorterReducer extends Reducer<NullWritable, PageRankWritable, NullWritable, PageRankWritable> implements Comparator<PageRankWritable>{

	private TreeSet<PageRankWritable> topPages;
	private long maxElements;

	/**
		*	load arguments and initialize variables
		*/	
	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		topPages=new TreeSet<PageRankWritable>(this);
		maxElements=context.getConfiguration().getLong(PageRankConstants.RES_KEY, 10);
	}

	/**
		*	add a result to the private data structure
		*/
	@Override
	public final void reduce(NullWritable oldKey, Iterable<PageRankWritable> oldValues, Context context) throws IOException, InterruptedException{
		for(PageRankWritable oldValue : oldValues){
			topPages.add(new PageRankWritable(oldValue));
			if(topPages.size()>maxElements) topPages.pollFirst();
		}
	}
	
	/**
		*	write results to context
		*/
	@Override
	protected final void cleanup(Context context) throws IOException, InterruptedException{
		for(PageRankWritable prw : topPages){
			context.write(NullWritable.get(), prw);
		}
	}
	
	/**
		*	template method handle to insert elements in the private structure
		*/
	public abstract int compare(PageRankWritable prw1, PageRankWritable prw2);
	
}
