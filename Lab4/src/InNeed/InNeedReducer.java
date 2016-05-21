/*
 * Reducer class for 
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

public class InNeedReducer extends Reducer<Text, Text, Text, Text>
{
    static long NumTrian = 0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
	boolean exist = false;
	for(Text val: values)
	{
	    if(val.toString().equals("&"))
		exist = true;
	    if(val.toString().equals("#") && exist)
		NumTrian++;
	}
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	context.write(new Text("Number of triangles: ") , new Text(String.valueOf(NumTrian)));
    }
}
