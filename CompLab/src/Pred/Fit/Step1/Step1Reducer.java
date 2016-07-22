/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:59
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class Step1Reducer extends Reducer<Text, Text, Text, Text>
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

	    double avg = 0, sum = 0, under = 0, k = 0;
        
        for(int i=0;i<14;i++)
            avg+=In[i];
        avg = avg/14;

        for(int i=0;i<14;i++)
        {
            under +=(i+1-7.5)*(i+1-7.5);
            sum +=(i+1-7.5)*(In[i]-avg);
        }
        k = sum/under;

        int res = (int) (In[13]+k);
        if(res<0)
        {
            res=In[13];
        }

        context.write(key,new Text(In[14] + "#" + res));
    }   
}
