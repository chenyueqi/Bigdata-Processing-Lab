/*
 * Combiner class
 * Project Name: 
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/4/22/21:36
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner extends Reducer<Text, LongWritable, Text, LongWritable>
{
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
    	long sum = 0;
    	for(LongWritable val : values)
    		sum += val.get();

    	LongWritable result = new LongWritable();
    	result.set(sum);
    	context.write(key, result);
    }
}
