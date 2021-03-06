/*
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Wei Liu
 * Time: 2016/5/25 22:30
 */

import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class StrongCheckMapper extends Mapper<Object, Text, Text, Text>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	String line =value.toString();
	String split[] = line.split(" ");
			
	Text outkey=new Text();
	Text outvalue=new Text();

	outkey.set(split[0] + "#" + split[1]);
	outvalue.set("@");

	context.write(outkey, outvalue);

    outkey.set(split[1] + "#" + split[0]);
	outvalue.set("*");

	context.write(outkey, outvalue);

    }
}

