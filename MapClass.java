package hibmproject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	static ArrayList<String> pos;
	static ArrayList<String> negative;
	
	
static
{
	
	
		
		
	try
	{
		
	
	String line="";
	pos=new ArrayList<>();
	InputStream fis=new FileInputStream("/home/cloudera/Desktop/peoplePulse/pdictionary");
	BufferedReader br=new BufferedReader(new InputStreamReader(fis));
	while((line=br.readLine())!=null)
	{
		pos.add(line);
		
	}
	}catch(Exception e){ e.printStackTrace();}
	
	System.out.println(pos);

	
	negative=new ArrayList<>();
	try
	{
	String line="";
	negative=new ArrayList<String>();
	InputStream fis=new FileInputStream("/home/cloudera/Desktop/peoplePulse/ndictionary");
	BufferedReader br=new BufferedReader(new InputStreamReader(fis));
	while((line=br.readLine())!=null)
	{
		negative.add(line);
		
	}
	}catch(Exception e){ e.printStackTrace();}
	System.out.println(negative);
	
}
	


String myline="";	
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

int result=0;
String line = value.toString();
StringTokenizer st=new StringTokenizer(line, ",");
while(st.hasMoreTokens())
{
	myline=st.nextToken();
}
System.out.println(myline);



int countneg=0;
int countpos=0;
StringTokenizer st1=new StringTokenizer(myline, " ");
while(st1.hasMoreTokens())
{
	String temp=st1.nextToken();
	for(int i=0;i<pos.size();i++)
	{
		if(pos.get(i).equals(temp))
		{
			countpos++;
		}
	}
	for(int i=0;i<negative.size();i++)
	{
		if(negative.get(i).equals(temp))
		{
			countneg++;
		}
		
	}
}
	if(countpos > countneg)
	{
		result=1;
	}
	else if(countpos < countneg)
	{
		result= -1;
	}
	else result= 0;
	
	String key1= "";
	
	if(result==1){
		key1="positive tweets";
	}
	else if(result== -1){
		key1= "negative tweets";
	}
	else if(result==0){
		key1= "neutral tweets";
	}
	IntWritable one = new IntWritable(1);
context.write(new Text(key1), one);
System.out.println(result);

}

}

