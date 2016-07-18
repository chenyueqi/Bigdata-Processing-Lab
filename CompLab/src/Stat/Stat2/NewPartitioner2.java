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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NewPartitioner2 extends HashPartitioner<Text, IntWritable>
{
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks)
    {
    	String term = new String();
    	/*partitioner according to word instead of <word#filename>*/
    	term = key.toString().split("#")[1];
        if(term.contains("@"))
            term = term.split("@")[1];
    	return super.getPartition(new Text(term), value, numReduceTasks);
    }
}
