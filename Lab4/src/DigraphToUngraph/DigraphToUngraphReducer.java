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
    static HashSet<String> postingList = new HashSet<String>();
    static Text CurrentItem = new Text(" ");

    public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException
    {
	if(!CurrentItem.equals(key) && (!CurrentItem.equals(" ")))
	{
	    int cnt = 0;
	    StringBuffer out = new StringBuffer();
	    for(String p:postingList)
	    {
		out.append(p);
		out.append(";");
		cnt++;
	    }
	    if(cnt > 0)
		context.write(CurrentItem, new Text(out.toString()));
	    postingList = new HashSet<String>();
	}
	CurrentItem = new Text(key);
	postingList.add(value.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	int cnt = 0;
	StringBuffer out = new StringBuffer();
	for(String p:postingList)
	{
	    out.append(p);
	    out.append(";");
	    cnt++;
	}
	if(cnt > 0)
	    context.write(CurrentItem, new Text(out.toString()));
    }
}
