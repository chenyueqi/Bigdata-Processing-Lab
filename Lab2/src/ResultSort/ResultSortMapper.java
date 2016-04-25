/*
 * Map class to sort Inverted Index's results
 * Project Name: Inverted Index Table for Hadoop(Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/25 14:24
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class ResultSortMapper extends Mapper<Object, Text, DoubleWritable, Text>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	String fileName = fileSplit.getPath().getName();
	String temp = new String();
	String line = value.toString();
	double average = Double.parseDouble(line.substring(line.indexOf("\t")+1, line.indexOf(",")));
	Text record = new Text();
	record.set(line);
	context.write(new DoubleWritable(average), record);
	
    }

}
