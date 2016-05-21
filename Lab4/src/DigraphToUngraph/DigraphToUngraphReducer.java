/*
 * Reducer class for translation directed graph to undirected graph
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Yueqi Chen (yueqichen.0x0@gmail.com)
 * Time: 2016/5/20 11:33
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;

public class DigraphToUngraphReducer extends Reducer<Text, Text, Text, Text>
{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
	StringBuilder out = new StringBuilder();

	for(Text value: values)
	{
	    if(out.indexOf(value.toString()) == -1)
		out.append(value.toString());
	    out.append("; ");
	}
	context.write(key, new Text(out.toString()));
    }
}
