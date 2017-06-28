package it.pad.sorter;

import it.pad.PageRankWritable;
import it.pad.sorter.PageRankWritableKeyComparator;

public class AscendingIdComparator extends PageRankWritableKeyComparator{
	
	@Override
	protected int compare(PageRankWritable prw1, PageRankWritable prw2){
		String id1=prw1.getSource();
		String id2=prw2.getSource();
		return id1.compareTo(id2);
	}
	
}
