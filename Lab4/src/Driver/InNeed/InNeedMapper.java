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
	String line = value.toString();

	String split[] = line.split("\t");
	
	String vertex[] = split[1].split(",");

	for(int i = 0 ; i < vertex.length ; i++)
	{
	    Text existedge = new Text();
	    existedge.set(split[0]+"#"+ vertex[i]);
	    context.write(existedge, new Text("&"));

	    for(int j = i+1; j < vertex.length ; j++)
	    {
		Text inneededge = new Text();
		if(vertex[i].compareTo(vertex[j]) < 0)
		    inneededge.set(vertex[i]+"#"+vertex[j]);
		else
		    inneededge.set(vertex[j]+"#"+vertex[i]);
		context.write(inneededge, new Text("#"));
	    }
	}
    }
}

