package it.pad.sorter;

import it.pad.PageRankWritable;
import it.pad.sorter.PageRankWritableKeyComparator;

/**
 * Defines the sorting of nodes based on the node id in ascending order
 */
public class AscendingStringIdComparator extends PageRankWritableKeyComparator{
	
	@Override
	protected int compare(PageRankWritable prw1, PageRankWritable prw2){
		String id1=prw1.getSource();
		String id2=prw2.getSource();
		return id1.compareTo(id2);
	}
	
}
