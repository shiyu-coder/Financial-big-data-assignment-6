package test;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Comparator;
import java.util.ArrayList;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {
    public static class Map
            extends Mapper<Object, Text, Text, Text> {
        private Text keyWord = new Text();
        private Text valueDocCount = new Text();
        private Set<String> stopWordList = new HashSet<String>();
        private Set<String> puncWordList = new HashSet<String>();

        protected void setup(Context context){
            // 停词文件路径
//        Path stopWordFile = new Path("D:\\Hadoop\\code\\test_hadoop\\input\\stop-word-list.txt");
            Path stopWordFile = new Path("hdfs://hadoop-master:9000/user/86137/input/stop-word-list.txt");
            // 标点符号文件路径
//        Path puctFile = new Path("D:\\Hadoop\\code\\test_hadoop\\input\\punctuation.txt");
            Path puctFile = new Path("hdfs://hadoop-master:9000/user/86137/input/punctuation.txt");
            readWordFile(stopWordFile, puctFile);
        }

        private void readWordFile(Path stopWordFile, Path pucFile){
            Configuration conf = new Configuration();
            try{
                FileSystem fs = FileSystem.get(URI.create(stopWordFile.toString()),conf);
                InputStream is = fs.open(new Path(stopWordFile.toString()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String stopWord = null;
                while ((stopWord = reader.readLine()) != null) {
                    stopWordList.add(stopWord);
                }
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
            try{
                FileSystem fs = FileSystem.get(URI.create(pucFile.toString()),conf);
                InputStream is = fs.open(new Path(pucFile.toString()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String pucWord = null;
                while ((pucWord = reader.readLine()) != null) {
                    puncWordList.add(pucWord);
                }
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
        }

//    private void readWordFile(Path stopWordFile, Path pucFile) {
//        try {
//            BufferedReader fis1 = new BufferedReader(new FileReader(stopWordFile.toString()));
//            String stopWord = null;
//            while ((stopWord = fis1.readLine()) != null) {
//                stopWordList.add(stopWord);
//            }
//        } catch (IOException ioe) {
//            System.err.println("Exception while reading stop word file '"
//                    + stopWordFile + "' : " + ioe.toString());
//        }
//        try{
//            BufferedReader fis2 = new BufferedReader(new FileReader(pucFile.toString()));
//            String pucWord = null;
//            while ((pucWord = fis2.readLine()) != null) {
//                puncWordList.add(pucWord);
//            }
//        } catch (IOException ioe) {
//            System.err.println("Exception while reading punc word file '"
//                    + pucFile + "' : " + ioe.toString());
//        }
//    }

        public void map(Object key, Text value, Context context){
            try{
                FileSplit fileSplit = (FileSplit)context.getInputSplit();
                String fileName = fileSplit.getPath().getName();
                StringTokenizer itr = new StringTokenizer(value.toString());
                while(itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    token = token.toLowerCase();
                    Pattern pattern = Pattern.compile("[\\d]");
                    Matcher matcher = pattern.matcher(token);
                    token = matcher.replaceAll("").trim();
                    if (token.length() >= 3 && !puncWordList.contains(token)) {
                        for(String puncWord: puncWordList){
                            token = token.replace(puncWord.substring(1), "");
                        }
                        if(token.length() >= 3 && !stopWordList.contains(token)){
                            keyWord.set(token + ":" + fileName);
                            valueDocCount.set("1");
                            context.write(keyWord, valueDocCount);
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text> {
        private Text wordCount = new Text();
        private Text wordDoc = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context){
            try{
                int sum = 0;
                for (Text value : values) {
                    sum += Integer.parseInt(value.toString());
                }
                int splitIndex = key.toString().indexOf(":");
                int splitFileName = key.toString().indexOf(".txt");
                wordDoc.set(key.toString().substring(0, splitIndex));
                wordCount.set(key.toString().substring(splitIndex + 1, splitFileName) + "#" + sum);
                context.write(wordDoc, wordCount);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context){
            try {
                ArrayList<Pair<Integer, String>> pairs = new ArrayList<>();
                Iterator<Text> it = values.iterator();
                StringBuilder all = new StringBuilder();
                for(;it.hasNext();) {
                    String slice = it.next().toString();
                    int splitIndex = slice.indexOf("#");
                    pairs.add(new Pair<>(Integer.parseInt(slice.substring(splitIndex+1)), slice));
                }
                Collections.sort(pairs, new Comparator<Pair<Integer, String>>() {
                    @Override
                    public int compare(Pair<Integer, String> o1, Pair<Integer, String> o2) {
                        if(o2.getKey().compareTo(o1.getKey())==0){
                            return o2.getValue().compareTo(o1.getValue());
                        }else{
                            return o2.getKey().compareTo(o1.getKey());
                        }
                    }
                });
                for(int i=0;i<pairs.size();i++){
                    all.append(pairs.get(i).getValue());
                    if(i!=pairs.size()){
                        all.append(", ");
                    }
                }
                context.write(new Text(key.toString() + ": " + all.toString()), NullWritable.get());
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "shakespeare/"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
