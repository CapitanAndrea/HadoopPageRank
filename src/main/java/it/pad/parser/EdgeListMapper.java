package it.pad.parser;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class EdgeListMapper extends ParserMapper{

	Text source=new Text();
	Text destination=new Text();

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]);
		destination.set(nodes[1]);
		context.write(source, destination);
	}
	
}
