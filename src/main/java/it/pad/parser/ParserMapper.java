package it.pad.parser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class ParserMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		parse(value.toString(), context);
	}
	
	public abstract void parse(String line, Context context) throws IOException, InterruptedException;
	
}
