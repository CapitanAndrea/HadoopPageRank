package it.pad.sorter;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import it.pad.PageRankWritable;

public class PageRankWritableGroupingComparator extends WritableComparator{

	protected PageRankWritableGroupingComparator() {
		super(PageRankWritable.class, true);
	}

//	All the compare methods return 0 so that all key-value pairs are given to the same reduce call
	@Override
	public final int compare(WritableComparable wc1, WritableComparable wc2){
		return 0;
	}

	@Override
	public final int compare(Object wc1, Object wc2){
		return 0;
	}

	@Override
	public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		return 0;
	}
	
}
