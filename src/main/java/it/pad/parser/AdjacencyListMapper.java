package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import it.pad.parser.ParserMapper;

public class AdjacencyListMapper extends ParserMapper{

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		if(line.startsWith("#")) return; //comment line
		String[] nodes=line.split("\\s");
		Text source=new Text(nodes[0]);
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
