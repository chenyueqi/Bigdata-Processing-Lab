/*
 * Map class for counting the number of triangles of a undirected graph
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/20 13:30
 */

import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class InNeedMapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	FileSplit fileSplit = (FileSplit) context.getInputSplit();

	Pattern p = Pattern.compile("(\t)|(,)+");
	String[] vertex = p.split(value.toString());


	for(int i = 1; i < vertex.length ; i++)
	{
	    Text existedge = new Text();
	    existedge.set(vertex[0]+"#"+vertex[i]);
	   
	    context.write(existedge, new Text("&"));
	}

	for(int i = 1 ; i < vertex.length ; i++)
	    for(int j = i+1; j < vertex.length ; j++)
	    {
		Text inneededge = new Text();
	    	long first = Long.parseLong(vertex[i]);
		long second = Long.parseLong(vertex[j]);

		if(vertex[i].compareTo(vertex[j]) < 0)
		    inneededge.set(vertex[i]+"#"+vertex[j]);
		else
		    inneededge.set(vertex[j]+"#"+vertex[i]);

		context.write(inneededge, new Text("#"));
	    }
    }
}

