package it.pad.parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{

        protected Text source=new Text();
        protected Text destination=new Text();
        protected final Text emptyText=new Text();
	
	/**
		*	this should emit one key-value for each edge and at least one key-emptyvalue for each node
		*/
	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line=value.toString();
		if(line.startsWith("#")) return; //comment line
		parse(line, context);
	}
	
	public abstract void parse(String line, Context context) throws IOException, InterruptedException;
	
}
