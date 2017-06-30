package it.pad.sorter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Comparator;

import it.pad.PageRankWritable;
import it.pad.PageRankConstants;

public class SorterReducer extends Reducer<PageRankWritable, PageRankWritable, NullWritable, PageRankWritable>{

	private long maxElements;

	@Override
	protected final void setup(Context context) throws IOException, InterruptedException{
		//get the number of records that has to be written in output
		maxElements=context.getConfiguration().getLong(PageRankConstants.RES_KEY, 10);
	}

	@Override
	public final void reduce(PageRankWritable key, Iterable<PageRankWritable> values, Context context) throws IOException, InterruptedException{
		long e=0;
		for(PageRankWritable prw : values){
			if(!e<maxElements) return;
			context.write(NullWritable.get(), prw);
			e++;
		}
	}
	
}
