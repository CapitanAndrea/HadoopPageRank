package it.pad.parser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import it.pad.parser.ParserMapper;

/**
 * Parser for adjacency lists.
 */
public class AdjacencyListMapper extends ParserMapper{

	@Override
	public void parse(String line, Context context) throws IOException, InterruptedException{
		String[] nodes=line.split("\\s");
		source.set(nodes[0]); //the first id of the line is the source node
		/* emit a pair for each edge */
		for(int i=1; i<nodes.length; i++){
			destination.set(nodes[i]);
			context.write(source, destination);
		}
		/* the id-empty value pair is not really needed if the node has at least one outgoing edge*/
		if(nodes.length==1){
			context.write(source, emptyText);
		}
	}
	
}
