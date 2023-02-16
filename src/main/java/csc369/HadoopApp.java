package csc369;

import java.io.IOException;

import javax.management.Query;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 
	    job.setReducerClass(UserMessages.JoinReducer.class);
	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountCountries".equalsIgnoreCase(otherArgs[0])) {
	   	job.setMapperClass(CountCountries.MapperImpl.class);
	    job.setReducerClass(CountCountries.ReducerImpl.class);
	    job.setOutputKeyClass(CountCountries.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountCountries.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	} else if ("CountryList".equalsIgnoreCase(otherArgs[0])) {
	 job.setMapperClass(UrlCountryList.MapperImpl.class);
	 job.setReducerClass(UrlCountryList.ReducerImpl.class);
	 job.setOutputKeyClass(UrlCountryList.OUTPUT_KEY_CLASS);
	 job.setOutputValueClass(UrlCountryList.OUTPUT_VALUE_CLASS);
	 job.setSortComparatorClass(UrlCountryList.SortComparator.class);
	 job.setGroupingComparatorClass(UrlCountryList.GroupingComparator.class);
	 FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	 FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	} else if ("CountryUrlCount".equalsIgnoreCase(otherArgs[0])) {
	 job.setMapperClass(CountryUrlCount.MapperImpl.class);
	 job.setReducerClass(CountryUrlCount.ReducerImpl.class);
	 job.setOutputKeyClass(CountryUrlCount.OUTPUT_KEY_CLASS);
	 job.setOutputValueClass(CountryUrlCount.OUTPUT_VALUE_CLASS);
	 FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	 FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	} else if ("SortCountDescending".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(SortDescending.MapperImpl.class);
		job.setReducerClass(SortDescending.ReducerImpl.class);
		job.setOutputKeyClass(SortDescending.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SortDescending.OUTPUT_VALUE_CLASS);
		job.setSortComparatorClass(SortDescending.SortComparator.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	} else if ("SortCountryCount".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(SortCountryCount.MapperImpl.class);
		job.setReducerClass(SortCountryCount.ReducerImpl.class);
		job.setOutputKeyClass(SortCountryCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SortCountryCount.OUTPUT_VALUE_CLASS);
		job.setSortComparatorClass(SortCountryCount.SortComparator.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	} else if ("CountryHostJoin".equalsIgnoreCase(otherArgs[0])) {
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, CountriesHosts.CountryMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, CountriesHosts.AccessMapper.class ); 
	    job.setReducerClass(CountriesHosts.JoinReducer.class);
	    job.setOutputKeyClass(CountriesHosts.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountriesHosts.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
