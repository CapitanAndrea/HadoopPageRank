package it.pad.parser;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class EdgeListMapper extends ParserMapper{

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]);
		destination.set(nodes[1]);
		context.write(source, destination); //write the pair for the edge
		//write a pair with key the destination of the edge and value an empty text so that the adjacency list will be built correctly
		context.write(destination, emptyText);
	}
	
}
