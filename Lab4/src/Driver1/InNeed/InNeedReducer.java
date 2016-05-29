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
    static int NumTrian = 0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
	ArrayList<String> list =new ArrayList<String>();
	HashMap<String, Boolean> map =new HashMap<String, Boolean>();
			
	for(Text val : values)
	{
		String v=val.toString();
				
		if(v.startsWith("a"))
		{
			String v2=v.substring(1);
			String sp[] =v2.split(",");
					
			for(int i=0;i<sp.length;i++)
			{
				map.put(sp[i], true);
			}
		}
		else
		{
			list.add(v);
		}
	}

	for(int i=0;i<list.size();i++)
	{
		if(map.containsKey(list.get(i)))
		{
			NumTrian++;
		}
	}
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	context.write(new Text("Number of triangles: ") , new Text(String.valueOf(NumTrian)));
    }
}
