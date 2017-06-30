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

	@Override
	public final int compare(Object wc1, Object wc2){
		return super.compare(wc1, wc2);
	}

	@Override
	public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		return super.compare(b1, s1, l1, b2, s2, l2);
	}
	
	/**
	 * Compare the two arguments to define a sorting strategy
	 */
	protected abstract int compare(PageRankWritable prw1, PageRankWritable prw2);
	
}
