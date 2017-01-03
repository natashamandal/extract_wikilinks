package extract_wikilinks;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ExtractWikilinks {

  public static class Mapper1 extends Mapper<Object, Text, Text , Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	SAXReader reader = new SAXReader();
    	Document document;
			try {
				document = reader.read(new StringReader(value.toString()));
		    	Element root = document.getRootElement();
		    	Iterator i,j;
		        for ( i = root.elementIterator( "title" ), j = root.element("revision").elementIterator("text") ; i.hasNext() && j.hasNext(); ) {
		            Element title = (Element) i.next();
		            String titlecontent = title.getText();
		            titlecontent = titlecontent.replaceAll("\\s+", "_");
		            context.write(new Text(titlecontent), new Text("!"));

		            	Element text = (Element) j.next();
		            	String textcontent = text.getText();
		    
		            	Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
		            			Matcher matcher = pattern.matcher(textcontent);
		            			while (matcher.find()) {
		            			//do the processing
		            				String k = matcher.group(1);
		            				k = k.replaceAll("\\s+", "_");
		            				context.write(new Text(k), new Text(titlecontent));
		            			}
		         
		        }
			} catch (DocumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	

    }
  }

  public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	Vector<String> v= new Vector<String>();
    	for(Text val: values)
    	{
    		v.add(val.toString());
    	}
    	//if(v.contains(new String("!"))&&v.size()>1)
    	if(v.contains(new String("!"))&&v.size()>=1)
    	{
    	//v.remove(new String("!"));	

	    	for(Iterator i = v.iterator(); i.hasNext();)
	    	{
	    		String n = (String)i.next();
            	Pattern pattern = Pattern.compile("[A-Za-z0-9]");
    			Matcher matcher = pattern.matcher(n);
	    		if(n.contains("!")&&!matcher.find())
	    		{
	    			context.write(key, new Text(""));
	    		}
	    		else	
	    		{
	    			context.write(new Text(n), key);
	    		}
	    	}
    	
    	}
    	
    }
  }
  
  public static class Mapper2 extends Mapper<Object, Text, Text , Text>{

      private Text word1 = new Text();
      private Text word2 = new Text();
      boolean s = false;
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	        /*StringTokenizer itr = new StringTokenizer(value.toString());*/
	        //while (itr.hasMoreTokens()) {
	          /*  word1.set(itr.nextToken());
	            if(itr.hasMoreTokens())
	            {
	            	word2.set(itr.nextToken());
	            	s = true;
	            }
	            
	            if(s)
	            {
	            	context.write(word1, word2);
	            }
	            else
	            {
	            	word2.set("");
	            	context.write(word1, word2);
	            }*/
	            //context.write(word, one);
	          //}
	    		//context.write(key, value);
	    	context.write(new Text(key.toString()), value);
	    }
  }	
  
  public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	    
	    	  List s = new ArrayList<String>();
	    	  for(Text val: values)
	    	  {
	    		  s.add(val.toString());
	    	  }
		      Set setItems = new LinkedHashSet(s);
		      s.clear();
		      s.addAll(setItems);	
	      String x="";	
	      Iterator i = s.iterator();

	      for(; i.hasNext();)
	      //while(i.hasNext())
	      {
	    	  //Text val = (Text) i.next();
	    	  String val = (String) i.next();
	    	  x += val.toString();
	    	  x += "\t";
	      }
	    	context.write(key, new Text(x));

	    	/*
	    	String x="";
	    	for(Text vals: values)
	    	{
	    		x+=vals;
	    		x+="\t";
	    	}
	    	context.write(key, new Text(x));*/
	    }	    	
  }    

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem.get(conf);
    conf.set("xmlinput.start", "<page");
    conf.set("xmlinput.end", "</page>");
    
    Job job = Job.getInstance(conf, "Extract Wikilinks");
    job.setJarByClass(ExtractWikilinks.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(XMLInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/temp"));
    job.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    FileSystem.get(conf2);
    Job job2 = Job.getInstance(conf2, "Extract Wikilinks");
	job2.setJarByClass(ExtractWikilinks.class);
    
	FileInputFormat.addInputPath(job2, new Path(args[1]+"/temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/graph"));
  
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}

