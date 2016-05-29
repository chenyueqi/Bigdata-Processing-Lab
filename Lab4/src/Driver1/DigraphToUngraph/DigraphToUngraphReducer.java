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
	
        TreeSet<String> set = new TreeSet<String>();
	
	for (Text val : values) 
	{
		String str = val.toString();
		set.add(str);
	}
			
	Iterator<String> it=set.iterator();
	String s = "";
		    
	while(it.hasNext())
	{
		s=s+it.next()+",";
	}
	
	Text outvalue=new Text();
	outvalue.set(s.substring(0, s.length()-1));    //截掉最后的逗号
	context.write(key, outvalue);
    }
}
