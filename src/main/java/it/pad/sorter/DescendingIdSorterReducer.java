package it.pad.sorter;

import it.pad.sorter.SorterReducer;
import it.pad.PageRankWritable;

public class DescendingIdSorterReducer extends SorterReducer{
	@Override
	public int compare(PageRankWritable prw1, PageRankWritable prw2){
		return -prw1.getSource().compareTo(prw2.getSource());
	}
}
