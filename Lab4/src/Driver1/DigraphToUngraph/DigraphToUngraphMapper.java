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
	String line=value.toString();
	String vertexs[]=line.split(" ");
	//long a = Long.parseLong(vertexs[0]);
	//long b = Long.parseLong(vertexs[1]);

    Text outkey=new Text();
    Text outvalue=new Text();
			
	if(vertexs[0].compareTo(vertexs[1])>0)
	{
		String newkey=vertexs[1] + "";
		outkey.set(newkey);

		String newvalue=vertexs[0]+"";
		outvalue.set(newvalue);

		context.write(outkey, outvalue);
	}
			
    else if(vertexs[0].compareTo(vertexs[1])<0)
	{
		String newkey=vertexs[0]+"";
		outkey.set(newkey);

		String newvalue=vertexs[1]+"";
		outvalue.set(newvalue);

		context.write(outkey, outvalue);
	}
    }
}

