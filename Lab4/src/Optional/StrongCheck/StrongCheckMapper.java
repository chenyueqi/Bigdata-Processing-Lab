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
	FileSplit fileSplit = (FileSplit) context.getInputSplit();
    String[] vertex = value.toString().split(" ");

    long first = Long.parseLong(vertex[0]);
    long second = Long.parseLong(vertex[1]);


    Text forwardedge = new Text();
    Text backedge = new Text();

	forwardedge.set(vertex[0]+"#"+vertex[1]);
    context.write(forwardedge, new Text("&"));
    backedge.set(vertex[1] + "#" + vertex[0]);
    context.write(backedge, new Text("#"));

    }
}

