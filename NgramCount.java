import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.MapWritable;

public class NgramCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MapWritable>{

            
            int N;
            String[] prev_n_words;
            HashMap<String, HashMap<String, Integer>> input_map = new HashMap<String, HashMap<String, Integer>>();
            public void setup(Context context){
                Configuration conf = context.getConfiguration(); //use this to pass parameter
                N = Integer.parseInt(conf.get("N"));
        	   // N = 2;    
		        prev_n_words = new String[N - 1];
            }
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                

		
		        String line = value.toString().replaceAll("[^A-Za-z]"," ");
		        StringTokenizer itr = new StringTokenizer(line);
                int num_itr = itr.countTokens() + N - 1;
                String[] words = new String[num_itr];
                int i,j;
		        for(i = 0; i < N - 1; i++){
                    words[i] = prev_n_words[i];
            	}
                int idx = N - 1;
                // Store line in words list
                while (itr.hasMoreTokens()) {
                    words[idx] = itr.nextToken(); 
                    idx = idx + 1;
                }

                // Copy words list in prev_n_words reversed
                for(i = num_itr - (N - 1), j = 0 ; j < N - 1; i++, j++){
                    prev_n_words[j] = words[i];
                }
                
                String word_list ="";
                // Doing N-grams according to each word
                for(i = 0; i <= num_itr - N ; i++){
                    if(words[i] != null){
                        String key_word = words[i];
                        HashMap<String, Integer> map = new HashMap<String, Integer>();
                        if (input_map.containsKey(key_word)){
                            map = input_map.get(key_word);
                        }
                        //Make N-gram words
                        for (j = i + 1 ; j < i + N; j++){
                            word_list = word_list + " " + words[j]; //concat words
                        }
                        if (map.containsKey(word_list)){
                            map.put(word_list, (int) map.get(word_list) + 1);
                        }else {
                            map.put(word_list, 1);
                        }
                        input_map.put(key_word, map);
                        word_list = "";
                    }
                }
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                for (Map.Entry<String, HashMap<String, Integer>> entry_out : input_map.entrySet()) {
                    String first_word = entry_out.getKey();
                    MapWritable after_word_map = new MapWritable();
                    for (Map.Entry<String, Integer> entry_in : entry_out.getValue().entrySet()) {
                        String after_word_list = entry_in.getKey();
                        Integer cnt = entry_in.getValue();
                        after_word_map.put(new Text(after_word_list), new IntWritable(cnt));
                        
                    }
                    context.write(new Text(first_word), after_word_map);
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<Text, MapWritable, Text, IntWritable> {
            private MapWritable result = new MapWritable();
            
            public void reduce(Text key, Iterable<MapWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                
                MapWritable result_map = new MapWritable();

                //Aggregate all the output of mapper
                for (MapWritable val : values) {
                    for (Map.Entry<Writable, Writable> map : val.entrySet()){
                        Text word_list = (Text)map.getKey();
                        IntWritable cnt = (IntWritable)map.getValue();

                        if(result_map.containsKey(word_list)){
                            result_map.put(word_list, new IntWritable(((IntWritable)result_map.get(word_list)).get() + cnt.get()));
                        }
                        else{
                            result_map.put(word_list, cnt);
                        }
                    }
                }
                for (Map.Entry<Writable, Writable> map : result_map.entrySet()) {
		    
                    Text word_list = (Text)map.getKey();
                    IntWritable cnt = (IntWritable)map.getValue();
                    context.write(new Text(key.toString() + word_list.toString()), cnt);
                }
                }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	    conf.set("N", args[2]);
        Job job = Job.getInstance(conf, "ngram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
