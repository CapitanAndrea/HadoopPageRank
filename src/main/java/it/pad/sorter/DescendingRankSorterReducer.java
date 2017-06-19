package it.pad.sorter;

import it.pad.sorter.SorterReducer;
import it.pad.PageRankWritable;

public class DescendingRankSorterReducer extends SorterReducer{

	public int compare(PageRankWritable prw1, PageRankWritable prw2){
		double r=prw2.getPageRank()-prw1.getPageRank();
		if(r>0) return 1;
		if(r<0) return -1;
		return prw1.getSource().compareTo(prw2.getSource());
	}
	
}
