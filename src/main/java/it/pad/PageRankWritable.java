package it.pad;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class PageRankWritable implements Writable {

	private Text sourceNode;
	private DoubleWritable pageRank;
	private Text adjacencyList;
	
	public PageRankWritable(){
		sourceNode=new Text();
		pageRank=new DoubleWritable();
		adjacencyList=new Text();
	}
	
	public PageRankWritable(Text source, boolean copyAdjacencyList){
		this();
		String[] parts=source.toString().split("\\s", 3);
		this.setSource(parts[0]);
		this.setPageRank(Double.parseDouble(parts[1]));
		if(copyAdjacencyList) this.setAdjacencyList(parts[2]);
	}
	
	public PageRankWritable(Text source){
		this(source, true);
	}
	
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
	
	public void setSource(String newSource){
		sourceNode.set(newSource);
	}
	
	public void setPageRank(double newPageRank){
		pageRank.set(newPageRank);
	}
	
	public void setAdjacencyList(String newAdjacencyList){
		adjacencyList.set(newAdjacencyList);
	}
	
	public String getSource(){
		return sourceNode.toString();
	}
	
	public double getPageRank(){
		return pageRank.get();
	}
	
	public String getAdjacencyList(){
		return adjacencyList.toString();
	}
	
	public void clearAdjacencyList(){
		adjacencyList.clear();
	}
	
	public boolean hasEmptyAdjacencyList(){
		return adjacencyList.getLength()==0;
	}
	
	@Override
	public String toString(){
		String separator="\t";
		StringBuilder sb=new StringBuilder();
		sb.append(sourceNode.toString());
		sb.append(separator);
		sb.append(pageRank.get());
		sb.append(separator);
		sb.append(adjacencyList.toString());
		return sb.toString();
	}
}
