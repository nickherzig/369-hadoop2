package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountriesHosts {
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for Country/HostFile file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String country = value.toString();
	    String out = "A\t"+country;
	    context.write(key, new Text(out));
	} 
    }

    // Mapper for access log file
    public static class AccessMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String text[] = value.toString().split(" ", 2);
		String hostname = text[0];
		String out = "B\t"+ text[1];
		context.write(new Text(hostname), new Text(out));
	    }
	}
    


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
	    ArrayList<String> countries = new ArrayList<String>();
	    ArrayList<String> messages = new ArrayList<String>();
        String text[];
        String a = "A";

	    for (Text val : values) {
            text = val.toString().split("\t");
            if (text[0].equals(a)) {
                countries.add(text[1]);
            }
            else{
                messages.add(text[1]);
            }
	    }

        for (String c : countries){
            for (String m : messages){
                context.write(new Text(c), new Text(key.toString() + " " + m));
            }
        }
	}
}
}
