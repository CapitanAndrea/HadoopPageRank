package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class AdjacencyListMapper extends ParserMapper{

	private Text source=new Text();

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]);
		Text destination=new Text();
		if(nodes.length==1){
			context.write(source, destination);
			return;
		}
		for(int i=1; i<nodes.length; i++){
			destination.set(nodes[i]);
			context.write(source, destination);
		}
	}
	
}
