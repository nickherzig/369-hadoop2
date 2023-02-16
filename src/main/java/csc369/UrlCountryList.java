package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlCountryList {
    public static final Class OUTPUT_KEY_CLASS = CountryUrlPair.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryUrlPair, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
        String country = "";
	    sa = sa[0].split(" ");
        for(int i = 0; i < sa.length - 1; i++){
            country += sa[i] + " ";
        }
        country = country.substring(0, country.length()-1);
	    context.write(new CountryUrlPair(country, sa[sa.length - 1]), new Text(country));
        }
    }

    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(CountryUrlPair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryUrlPair pair = (CountryUrlPair) wc1;
            CountryUrlPair pair2 = (CountryUrlPair) wc2;
            return pair.getUrl().compareTo(pair2.getUrl());
        }
    }

    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(CountryUrlPair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryUrlPair pair = (CountryUrlPair) wc1;
            CountryUrlPair pair2 = (CountryUrlPair) wc2;
            if (pair.getUrl().compareTo(pair2.getUrl()) == 0){
                return pair.getCountry().compareTo(pair2.getCountry());
            }
            return pair.getUrl().compareTo(pair2.getUrl());
        }
    }

    public static class ReducerImpl extends Reducer<CountryUrlPair, Text, Text, Text> {
    
        @Override
	protected void reduce(CountryUrlPair pair, Iterable<Text> c,
			      Context context) throws IOException, InterruptedException {
            String result = "";
        
            for (Text country : c){
                result+=(country + ",");
            }
            result = result.substring(0, result.length()-1);
            context.write(pair.getUrl(), new Text(result));
       }
    }
}
