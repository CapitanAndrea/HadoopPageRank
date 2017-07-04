package it.pad.sorter;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import it.pad.PageRankWritable;

public abstract class PageRankWritableKeyComparator extends WritableComparator{

	protected PageRankWritableKeyComparator() {
		super(PageRankWritable.class, true);
	}

	@Override
	public final int compare(WritableComparable wc1, WritableComparable wc2){
		return compare((PageRankWritable)wc1, (PageRankWritable)wc2);
	}

	/**
	 * Compare the two arguments to define a sorting strategy
	 */
	protected abstract int compare(PageRankWritable prw1, PageRankWritable prw2);
	
}
