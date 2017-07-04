package it.pad;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class PageRankWritable implements WritableComparable<PageRankWritable> {

	private Text sourceNode;
	private DoubleWritable pageRank;
	private Text adjacencyList;
	
	/**
	 * Creates a new instance with default values
	 */
	public PageRankWritable(){
		sourceNode=new Text();
		pageRank=new DoubleWritable();
		adjacencyList=new Text();
	}
	
	/**
	 * Creates a new instance by parsing the source argument, selectively initializing the adjacency list
	 */
	public PageRankWritable(Text source, boolean copyAdjacencyList){
		this();
		String[] parts=source.toString().split("\\s", 3);
		this.setSource(parts[0]);
		this.setPageRank(Double.parseDouble(parts[1]));
		if(copyAdjacencyList){
			try{
				this.setAdjacencyList(parts[2]);
			} catch(ArrayIndexOutOfBoundsException e){} //if there is no adjacency list to store just go on
		}
	}
	
	/**
	 * Creates a new instance by parsing the source argument
	 */
	public PageRankWritable(Text source){
		this(source, true);
	}
	
	/**
	 * Creates a new instance by copying the values in the source object
	 */
	public PageRankWritable(PageRankWritable source){
		this();
		this.setSource(source.getSource());
		this.setPageRank(source.getPageRank());
		this.setAdjacencyList(source.getAdjacencyList());
	}
	
	@Override
	public void write(DataOutput output) throws IOException{
		sourceNode.write(output);
		pageRank.write(output);
		adjacencyList.write(output);
	}
	
	@Override
	public void readFields(DataInput input) throws IOException{
		sourceNode.readFields(input);
		pageRank.readFields(input);
		adjacencyList.readFields(input);
	}
	
	public static PageRankWritable read(DataInput input) throws IOException{
		PageRankWritable prw=new PageRankWritable();
		prw.readFields(input);
		return prw;
	}
	
	/* setters and getters */
	public void setSource(String newSource){
		sourceNode.set(newSource);
	}
	
	public void clearSource(){
		sourceNode.clear();
	}
	
	public boolean hasEmptySource(){
		return sourceNode.getLength()==0;
	}
	
	public String getSource(){
		return sourceNode.toString();
	}
	
	public void setPageRank(double newPageRank){
		pageRank.set(newPageRank);
	}
	
	public double getPageRank(){
		return pageRank.get();
	}
	
	public void setAdjacencyList(String newAdjacencyList){
		adjacencyList.set(newAdjacencyList);
	}
	
	public void clearAdjacencyList(){
		adjacencyList.clear();
	}
	
	public boolean hasEmptyAdjacencyList(){
		return adjacencyList.getLength()==0;
	}
	
	public String getAdjacencyList(){
		return adjacencyList.toString();
	}
	
	@Override
	public String toString(){
		String separator="\t";
		StringBuilder sb=new StringBuilder();
		sb.append(sourceNode.toString());
		sb.append(separator);
		sb.append(pageRank.get());
		if(!hasEmptyAdjacencyList()){
			sb.append(separator);
			sb.append(adjacencyList.toString());
		}
		return sb.toString();
	}

	@Override
	public int compareTo(PageRankWritable prw){
		return 0;
	}
}
