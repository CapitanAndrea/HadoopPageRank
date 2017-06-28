package it.pad.sorter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import it.pad.PageRankWritable;

public class SorterMapper extends Mapper<LongWritable, Text, PageRankWritable, PageRankWritable>{

	private PageRankWritable prw=new PageRankWritable();

	@Override
	public final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		prw=new PageRankWritable(value, false);	//delete the adjacency list because it is not needed at this stage		
		context.write(prw, prw);
	}
}
