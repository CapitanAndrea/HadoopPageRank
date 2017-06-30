package it.pad.sorter;

import it.pad.PageRankWritable;
import it.pad.sorter.PageRankWritableKeyComparator;

/**
 * Defines the sorting of nodes based on PageRank value in descending order
 */
public class DescendingRankComparator extends PageRankWritableKeyComparator{
	
	@Override
	protected int compare(PageRankWritable prw1, PageRankWritable prw2){
		double rank1=prw1.getPageRank();
		double rank2=prw2.getPageRank();
		if(rank1==rank2) return 0;
		return rank1>rank2 ? -1 : 1;
	}
	
}
