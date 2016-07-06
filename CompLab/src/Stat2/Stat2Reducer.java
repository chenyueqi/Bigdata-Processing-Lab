/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:59
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Stat2Reducer extends Reducer<Text, IntWritable, Text, Text>
{
    static List<String> postingList = new ArrayList<String>();
    static Text CurrentItem = new Text(" ");

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
		int sum =0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		Text result = new Text();
		result.set(key.toString()+":"+sum);
		context.write(result, new Text(""));
    }
}
