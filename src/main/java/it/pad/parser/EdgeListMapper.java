package it.pad.parser;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class EdgeListMapper extends ParserMapper{

	private Text source=new Text();
	private Text destination=new Text();
	private final Text emptyText=new Text();

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]);
		destination.set(nodes[1]);
		context.write(source, destination);
		//write an edge with source the destination of the edge and destination an empty text so that the adjacency list will be built correctly
		context.write(destination, emptyText);
	}
	
}
