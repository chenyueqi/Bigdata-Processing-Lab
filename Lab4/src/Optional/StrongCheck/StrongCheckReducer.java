/*
 * Reducer class for 
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Wei Liu
 * Time: 2016/5/20 22:30
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;

public class StrongCheckReducer extends Reducer<Text, Text, Text, Text>
{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
	boolean exist = false;
	for(Text val: values)
	{
	    if(val.toString().equals("&"))
		exist = true;
	    if(val.toString().equals("#") && exist)
        {
            String[] vertex = key.toString().split("#");
            if(vertex[0].compareTo(vertex[1])<0)
                context.write(new Text(vertex[0]), new Text(vertex[1]));
            else if(vertex[0].compareTo(vertex[1])>0)
                context.write(new Text(vertex[1]), new Text(vertex[0]));
        }

	}
    }

}
