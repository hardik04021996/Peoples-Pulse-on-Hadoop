package hibmproject;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {



@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {

	

int count=0;
for (IntWritable value: values) {
count= count+value.get();
}
context.write(key, new IntWritable(count));
System.out.println("["+key+"]"+"   "+"("+count+")");


}
}
