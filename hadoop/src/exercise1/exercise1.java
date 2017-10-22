package exercise1;

//import java.io.File;
import java.io.IOException;
//import java.lang.ProcessBuilder.Redirect;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparator;


public class exercise1 extends Configured implements Tool {
	
   public static void main(String[] args) throws Exception {
     
      int exit = ToolRunner.run(new exercise1(), args);
      System.exit(exit);
   }

   @Override
   public int run(String[] args) throws Exception {
	  JobControl jobControl = new JobControl("chained"); 
	  Configuration conf1 = getConf();
	  conf1.set("mapreduce.map.output.compress", "true");
    
      Job job = Job.getInstance(conf1);
      job.setJarByClass(exercise1.class);
      job.setJobName("Find StopWords");
      
      
     //   FileInputFormat.addInputPath(job, new Path("./docs/pg*.txt"));
     //   FileOutputFormat.setOutputPath(job, new Path("./outdocs/"));
      FileInputFormat.addInputPath(job, new Path(args[0]));
     //   FileInputFormat.setInputPaths(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1] + "/exer1"));
      
      job.setMapperClass(wordMap.class);
      job.setCombinerClass(ov4kReduce.class);
      job.setReducerClass(ov4kReduce.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

     // job.setNumReduceTasks(10);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
    

      ControlledJob controlledJob = new ControlledJob(conf1);
      controlledJob.setJob(job);
      jobControl.addJob(controlledJob);

      //JOB No2 Gia na kanei invert ta key,value kai na kanei ta stopwords sort kata syxnotita >> no1=to top stopword  p.x 1224 and 1020 as 
      Configuration conf2 = getConf();
      conf2.set("mapred.textoutputformat.separator", ",");
      Job job1 = Job.getInstance(conf2);
      job1.setJarByClass(exercise1.class);
      job1.setJobName("StopWords Invert");
      
   //  FileInputFormat.addInputPath(job1, new Path("./outdocs/part-r-*"));
 //   FileOutputFormat.setOutputPath(job1, new Path("./outdocs2/"));
           FileInputFormat.addInputPath(job1, new Path(args[1] + "/exer1"));
      //   FileInputFormat.setInputPaths(job1, new Path(args[1] + "/exer1"));
         FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/exer1out"));

      
      job1.setMapperClass(WordMapper2.class);
      job1.setReducerClass(SumReducer2.class);
      job1.setCombinerClass(SumReducer2.class);
      
      job1.setOutputKeyClass(IntWritable.class);
      job1.setOutputValueClass(Text.class);
      job1.setInputFormatClass(KeyValueTextInputFormat.class);

      job1.setSortComparatorClass(IntComparator.class);
      ControlledJob controlledJob1 = new ControlledJob(conf2);
      controlledJob1.setJob(job1);

      // make job1 dependent on job && add the job to the job control
      controlledJob1.addDependingJob(controlledJob);
      jobControl.addJob(controlledJob1);
      
      Thread jobControlThread = new Thread(jobControl);
      jobControlThread.start();

      while (!jobControl.allFinished()) {
    	    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
    	    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
    	    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
    	    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
    	    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
    	try {
    	    Thread.sleep(5000);
    	    } catch (Exception e) {

    	    }

    	  } 

   /*   if(job1.isComplete())
      {
    	
      String[] command = {"hadoop","fs","-cat","pg*.txt","combined.txt"};
      					
   	  ProcessBuilder processBuilder = new ProcessBuilder(command);
   	//  processBuilder.directory(new File("/home/cloudera/workspace/hadoop/outdocs2/final/"));
   	 
      processBuilder.redirectErrorStream(true);
      processBuilder.redirectOutput(Redirect.INHERIT);
      Process p = processBuilder.start();
      p.waitFor();

      
      }*/
    	   System.exit(0);  
    	   return (job.waitForCompletion(true) ? 0 : 1); 
   }
   
   public static class wordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
         for (String token: value.toString().split("\\s+|-{2,}+")) {
        	word.set(token.replaceAll("[^A-Za-z]+","").toLowerCase());
        	if(!(word.toString().isEmpty()))
            context.write(word, ONE);
         }
      }
   }

   public static class ov4kReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         if (sum>4000){
             
        	context.write(key, new IntWritable(sum));
         }
      }
   }
   
   public static class WordMapper2 extends Mapper< Text, Text, IntWritable, Text> {

		  IntWritable frequency = new IntWritable();

		  @Override
		  public void map(Text key, Text value, Context context)
		    throws IOException, InterruptedException {
			  
		    int newVal = Integer.parseInt(value.toString());
		    frequency.set(newVal);
		    context.write(frequency, key);
		  }
		}

	public static class SumReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {

		  Text word = new Text();

		  @Override
		  public void reduce(IntWritable key, Iterable<Text> values, Context context)
		        throws IOException, InterruptedException {

		    for (Text value : values) {
		        word.set(value);
		        context.write(key, word);
		    }
		  }
		}

	public static class IntComparator extends WritableComparator {

		  public IntComparator() {
		    super(IntWritable.class);
		  }

		  @Override
		  public int compare(byte[] b1, int s1, int l1, byte[] b2,int s2, int l2) {
		    Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
		    Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
		    return v1.compareTo(v2) * (-1);
		  }
		}
   
   
}



