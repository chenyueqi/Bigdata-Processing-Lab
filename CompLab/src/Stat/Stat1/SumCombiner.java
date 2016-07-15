/*
 * Combiner class
 * Project Name: Comprehensive Lab
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/4/22/21:36
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
    	int sum = 0;
    	for(IntWritable val : values)
    		sum += val.get();

    	IntWritable result = new IntWritable();
    	result.set(sum);
    	context.write(key, result);

    }
}
