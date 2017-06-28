package it.pad.sorter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Comparator;

import it.pad.PageRankWritable;
import it.pad.PageRankCounters;
import it.pad.PageRankConstants;

public class SorterReducer extends Reducer<PageRankWritable, PageRankWritable, NullWritable, PageRankWritable>{

	//TODO: capire se c'è un modo per far arrivare i valori al reducer già ordinati (secondary sorting o qualcosa del genere)
	private long maxElements;

	/**
		*	load arguments and initialize variables
		*/
	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		maxElements=context.getConfiguration().getLong(PageRankConstants.RES_KEY, 10);
	}

	/**
		*	add a result to the private data structure
		*/
	@Override
	public final void reduce(PageRankWritable key, Iterable<PageRankWritable> values, Context context) throws IOException, InterruptedException{
		long e=0;
		for(PageRankWritable prw : values){
			if(e++<maxElements) context.write(NullWritable.get(), prw);
		}
	}
	
}
