/**
 * 
 */

/**
 * @author siddgarg
 *
 */
import java.io.IOException;

import java.util.*;

import java.util.Map.Entry;



import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class movie {



public static class Map extends

Mapper<LongWritable, Text, Text, Text> {




@Override

public void map(LongWritable key, Text value, Context context)

throws IOException, InterruptedException {

String line = value.toString();

String[] moviedata=line.split("::");

String strmoviegenres=moviedata[2];

String movieName=moviedata[1];

String movieRating=moviedata[6];

String movieNameWithRating=movieName+"//"+movieRating;

if(!strmoviegenres.isEmpty())

{

String[] allmoviegenres=strmoviegenres.split(",");


for(int i=0;i<allmoviegenres.length;i++)

{


context.write(new Text(allmoviegenres[i]),new Text(movieNameWithRating));


}


}

}

}




public static class Reduce extends

Reducer<Text, Text, Text, Text> {



@Override

public void reduce(Text key, Iterable<Text> values,

Context context) throws IOException, InterruptedException {


// create map to store

List<Integer> rating = new ArrayList<Integer>();

HashMap<String, List<Integer>> map = new HashMap<String, List<Integer>>();





for(Text value:values)

{

String movieNameWithRating= value.toString();

String[] movieNameAndRatingArray=movieNameWithRating.split("//");







        if (!map.containsKey(movieNameAndRatingArray[0])) // doesn't exist

        {

            {

            rating = new ArrayList<Integer>();

        rating.add(Integer.parseInt(movieNameAndRatingArray[1]));

            }

        }

        else

        {

        rating = map.get(movieNameAndRatingArray[0]);

                  //System.out.println("The square root of " + rating );



            rating.add(Integer.parseInt(movieNameAndRatingArray[1]));

           

        }

map.put(movieNameAndRatingArray[0], rating);

}

HashMap<String, Float> mapForAvg = new HashMap<String, Float>();



for (String movieName : map.keySet()) {

List<Integer> ratingValue = map.get(movieName);

            int sum = 0;

for(int i = 0; i < ratingValue.size(); i++)

{          

sum=sum+ratingValue.get(i);

}

float avg=(float)sum/ratingValue.size();



mapForAvg.put(movieName, avg);

}

        Float maxValueInMap=(Collections.max(mapForAvg.values()));  // This will return max value in the Hashmap

        for (Entry<String, Float> entry : mapForAvg.entrySet()) {  // Itrate through hashmap

            if (entry.getValue()==maxValueInMap) {

context.write(key, new Text(entry.getKey()+ " "+ maxValueInMap));



                // Print the key with max value

            }









}


}

}

public static void main(String[] args) throws Exception {


Configuration conf = new Configuration();



Job job = Job.getInstance(conf, "movie");


job.setMapperClass(Map.class);

job.setReducerClass(Reduce.class);



job.setOutputKeyClass(Text.class);

job.setOutputValueClass(Text.class);


job.setInputFormatClass(TextInputFormat.class);

job.setOutputFormatClass(TextOutputFormat.class);



FileInputFormat.addInputPath(job, new Path(args[0]));

FileOutputFormat.setOutputPath(job, new Path(args[1]));



job.waitForCompletion(true);


}

}