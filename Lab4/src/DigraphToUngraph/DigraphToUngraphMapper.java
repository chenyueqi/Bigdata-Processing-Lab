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

	String[] vetex = value.toString().split(" ");
	long first = Long.parseLong(vetex[0]);
	long second = Long.parseLong(vetex[1]);

	Text a = new Text();
	Text b = new Text();

	if(first < second)
	{
	    a.set(vetex[0]);
	    b.set(vetex[1]);
	}
	else if(second < first)
	{
	    a.set(vetex[1]);
	    b.set(vetex[0]);
	}

	context.write(a, b);
    }

}

