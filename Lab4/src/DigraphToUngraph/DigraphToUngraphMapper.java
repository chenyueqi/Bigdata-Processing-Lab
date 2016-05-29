/*
 * Map class to translation directed graph to undirected graph
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/20 11:06
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class DigraphToUngraphMapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	FileSplit fileSplit = (FileSplit) context.getInputSplit();

	String[] vertex = value.toString().split(" ");
	Text a = new Text();
	Text b = new Text();

	if(vertex[0].compareTo(vertex[1]) > 0)
	{
	    a.set(vertex[1]);
	    b.set(vertex[0]);
	    context.write(a, b);
	}
	else if(vertex[0].compareTo(vertex[1]) < 0)
	{
	    a.set(vertex[0]);
	    b.set(vertex[1]);
	    context.write(a, b);
	}
    }
}

