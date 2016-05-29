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
	String line =value.toString();
	String split[] = line.split("\t");
			
	Text outkey=new Text();
	Text outvalue=new Text();

	outkey.set(split[0]);
	String value1="a"+split[1];
	outvalue.set(value1);

	context.write(outkey, outvalue);
			
	if(split[1].contains(","))
	{
		String vertexs2[] = split[1].split(",");
		for(int i=0;i<vertexs2.length-1;i++)
		{
			for(int j=i+1;j<vertexs2.length;j++)
			{
				outkey.set(vertexs2[i]);
				outvalue.set(vertexs2[j]);
				context.write(outkey,outvalue);
			}
		}
	}
    }
}

