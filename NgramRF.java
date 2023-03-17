import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.math.*;


public class NgramRF {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MapWritable>{
            int N;
            String[] prev_n_words;
            HashMap<String, HashMap<String, Integer>> input_map = new HashMap<String, HashMap<String, Integer>>();
            public void setup(Context context){
                Configuration conf = context.getConfiguration(); //use this to pass parameter
                N = Integer.parseInt(conf.get("N"));
		        prev_n_words = new String[N - 1];
            }
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                
                String line = value.toString();
                StringTokenizer itr = new StringTokenizer(line.replaceAll("[^A-Za-z]", " "));
                int num_itr = itr.countTokens() + N - 1;
                String[] words = new String[num_itr];
                int i, j;
                for(i = 0; i < N - 1; i++){
                    words[i] = prev_n_words[i];
            	}
                int idx = N - 1;
                while (itr.hasMoreTokens()) {
                    words[idx] = itr.nextToken(); 
                    idx = idx + 1;
                }
                for(i = num_itr - (N - 1), j = 0 ; j < N - 1; i++, j++){
                    prev_n_words[j] = words[i];
                }
                
                String word_list ="";
                for(i = 0; i <= num_itr - N ; i++){
                    if(words[i] != null){
                        String key_word = words[i];
                        HashMap<String, Integer> inner_map = new HashMap<String, Integer>();
                        if (input_map.containsKey(key_word)){
                            inner_map = input_map.get(key_word);
                        }
                        for (j = i + 1 ; j < i + N; j++){
                            word_list = word_list + " " + words[j]; //concat words
                        }
                        // Put "*" for containing total ngram count according to each word
                        if (inner_map.containsKey(word_list)){
                            inner_map.put(word_list, (int) inner_map.get(word_list) + 1);
                            inner_map.put("*", (int) inner_map.get("*") + 1);
                            
                        }else {
                            if (inner_map.containsKey("*")){
                                inner_map.put("*", (int) inner_map.get("*") + 1);
                            }else{
                                inner_map.put("*",1);
                            }
                            inner_map.put(word_list, 1);
                            
                        }
                        input_map.put(key_word, inner_map);
                        word_list = "";
                    }
                }
                
            }
            
            public void cleanup(Context context) throws IOException, InterruptedException {
                for (Map.Entry<String, HashMap<String, Integer>> outer_entry : input_map.entrySet()) {
                    String first_word = outer_entry.getKey();
                    MapWritable after_word_map = new MapWritable();
                    for (Map.Entry<String, Integer> inner_entry : outer_entry.getValue().entrySet()) {
                        String after_word_list = inner_entry.getKey();
                        Integer cnt = inner_entry.getValue();
                        after_word_map.put(new Text(after_word_list), new IntWritable(cnt));
                        
                    }
                    context.write(new Text(first_word), after_word_map);
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<Text, MapWritable, Text, DoubleWritable> {
            double th1;
            public void reduce(Text key, Iterable<MapWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int total = 1;
		        
                MapWritable final_map = new MapWritable();
                Configuration conf = context.getConfiguration();
                th1 = Double.parseDouble(conf.get("theta"));
                for (MapWritable val : values) {
                    for (Map.Entry<Writable, Writable> map : val.entrySet()){
                        Text word_list = (Text)map.getKey();
                        IntWritable cnt = (IntWritable)map.getValue();
			
                        //total = total + cnt.get();
                        if(final_map.containsKey(word_list)){
                            final_map.put(word_list, new IntWritable(((IntWritable)final_map.get(word_list)).get() + cnt.get()));
                        }
                        else{
				                final_map.put(word_list, cnt);
                        }
                    }
                }
                for (Map.Entry<Writable, Writable> map : final_map.entrySet()) {
                    Text word_list = (Text)map.getKey();
	                IntWritable cnt = (IntWritable)map.getValue();
                    //getting total count
                    if (word_list.toString().equals("*")){
                        total = cnt.get();
                    }    
                }
                for (Map.Entry<Writable, Writable> map : final_map.entrySet()) {
                    
                    Text word_list = (Text)map.getKey();
	                IntWritable cnt = (IntWritable)map.getValue();

                    //Getting Relatvie frequency using total
                    Text outputText = new Text( key.toString() + word_list.toString());
                    double rf = (double) cnt.get() / total;
                                        
                    if( (rf >= th1) && !(word_list.toString().equals("*"))){
                        DoubleWritable outputvalue = new DoubleWritable(rf);
                        context.write(outputText, outputvalue);
                    }
                }
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	    conf.set("N", args[2]);
        conf.set("theta",args[3]);
        Job job = Job.getInstance(conf, "ngram countrf");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
