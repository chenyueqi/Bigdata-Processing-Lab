/*
 * partitioner class
 * Project Name:
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/7/6 14:18
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NewPartitioner extends HashPartitioner<Text, LongWritable>
{
    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks)
    {
    	String term = new String();

    	term = key.toString().split("#")[0];
    	return super.getPartition(new Text(term), value, numReduceTasks);
    }
}
