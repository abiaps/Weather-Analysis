import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WeatherAnalysis {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String inputLine = value.toString();
			// skips the header line which is string
			if (inputLine.substring(0, 1).matches("[0-9]")) {
				String arr[] = inputLine.split("\\s+");
				Text outputKey = new Text();
				Text outputValue = new Text();
				// outputKey = stationid + yearmoday
				outputKey.set(arr[0] + "," + arr[2].substring(0, 8));
				double temperature = Double.parseDouble(arr[3].toString());
				double wind = Double.parseDouble(arr[12].toString());
				double dew_point = Double.parseDouble(arr[4].toString());
				outputValue.set(temperature + "," + wind + "," + dew_point);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double tempSum = 0.0;
			double windSum = 0.0;
			double dewSum = 0.0;

			String[] value;
			Text outputValue = new Text();

			for (Text val : values) {
				value = val.toString().split(",");
				tempSum += Double.parseDouble(value[0]);
				windSum += Double.parseDouble(value[1]);
				dewSum += Double.parseDouble(value[2]);
			}
			outputValue.set(tempSum + "," + windSum + "," + dewSum);
			context.write(key, outputValue);
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Text outputKey = new Text();
			Text outputValue = new Text();

			String arr[] = value.toString().split("\\s+");
			outputKey.set(arr[0]);
			String[] line = arr[1].split(",");
			outputValue.set(Double.parseDouble(line[0]) + ","
					+ Double.parseDouble(line[1]) + ","
					+ Double.parseDouble(line[2]));
			context.write(outputKey, outputValue);

		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// finding average temperature, windspeed and dewpoint for each day
			// of each month of each year
			for (Text val : values) {
				String[] line = val.toString().split(",");
				Text outputValue = new Text();
				// total temp/windspeed/dewpoint divided by 24hours
				double tempAvg = Math.round(Double.parseDouble(line[0]) / 24);
				double windAvg = Math.round(Double.parseDouble(line[1]) / 24);
				double dewAvg = Math.round(Double.parseDouble(line[2]) / 24);
				outputValue.set(tempAvg + "," + windAvg + "," + dewAvg);
				context.write(key, outputValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job1 = Job.getInstance(conf, "Job1");
		job1.setJarByClass(WeatherAnalysis.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Job2");
		job2.setJarByClass(WeatherAnalysis.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.waitForCompletion(true);

	}

}
