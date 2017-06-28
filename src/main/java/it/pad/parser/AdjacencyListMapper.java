package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class AdjacencyListMapper extends ParserMapper{

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]);
		context.write(source, emptyText);
		for(int i=1; i<nodes.length; i++){
			destination.set(nodes[i]);
			context.write(source, destination);
		}
	}
	
}
