package it.pad.parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{

	protected Text source=new Text();
  protected Text destination=new Text();
  protected final Text emptyText=new Text();
	
	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line=value.toString();
		if(line.startsWith("#")) return; //comment line
		parse(line, context);
	}
	
	/**
	 *	Extract edges from the passed line and write them to the context.
	 *	This should emit one pair source-destination for each edge and at least one pair id-emptyvalue for each node
	 */
	public abstract void parse(String line, Context context) throws IOException, InterruptedException;
	
}
