/*
 * Combiner class
 * Project Name: 
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/4/22/21:36
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner1 extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	int[] In = new int[15];
    	for(int i=0;i<15;i++)
    		In[i] = 0;

        for(Text val:values)
        {
            String[] Ins = val.toString().split(" ");
            for(int i=0;i<15;i++)
                In[i] += Integer.parseInt(Ins[i]);
        }

	context.write(key,new Text(
                        In[0] + " " +
                        In[1] + " " +
                        In[2] + " " +
                        In[3] + " " +
                        In[4] + " " +
                        In[5] + " " +
                        In[6] + " " +
                        In[7] + " " +
                        In[8] + " " +
                        In[9] + " " +
                        In[10] + " " +
                        In[11] + " " +
                        In[12] + " " +
                        In[13] + " " + In[14]
                        ));
    }
}
