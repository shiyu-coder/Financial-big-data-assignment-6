# 金融大数据-作业6

施宇 191250119

## 题目要求

在作业5的数据集基础上完成莎士比亚文集单词的倒排索引，输出按字典序对单词进行排序，单词的索引按照单词在该文档中出现的次数从大到小排序。单词忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。输出格式如下：

```shell
单词1: 文档i#频次a, 文档j#频次b...
单词2: 文档m#频次x, 文档n#频次y...
...
```

## 基本思路

题目要求完成多个作品的单词的倒排索引。整个过程需要两次整合过程，第一次是将同一部作品的同一个单词合并完成计数工作，第二次是对同一个单词在不同作品中的词频整合在一起并进行排序。因此在Combine过程中完成第一个整合过程，在Reduce过程中完整第二个整合过程。

在map阶段，对输入的每一行文本按空格划分得到单词后，转为小写，剔除标点符号、停词、数字，并检查单词长度是否大于等于3。如果单词满足要求，便令key的格式为"单词名:文件名"，value为1。

在combine阶段，reduce函数接收到每个单词在每个作品中的出现次数，这里对次数求和得到每个单词在每个作品中的词频。下一步需要对每个单词在不同作品中的词频进行排序，因此设置reduce输出的key为单词名，value的格式为"作品名#词频"。

在reduce阶段，将输入的value拆分成键值对，key为词频，value为作品名，将该键值对加入到数组中（这里使用ArrayList结构作为数组），然后对该数组按照规定的方式排序：先根据词频从大到小排序，词频相同的根据作品名的字典序从大到小排序。

对于输出格式方面，由于通过reduce输出时，key和value之间会自动加上一个制表符进行分隔，而要求的格式中，单词后跟随的是冒号，因此设置输出的value为NullWritable对象，仅通过输出key来满足格式要求。

## 运行程序

整个程序只有一个job，且程序只有一个输出文件，即输出目录下的part-r-00000文件。

在集群上运行：

![image1](https://github.com/shiyu-coder/Financial-big-data-assignment-6/blob/master/image/1.png)

![image2](https://github.com/shiyu-coder/Financial-big-data-assignment-6/blob/master/image/2.png)

## 性能与可拓展性分析

本程序仅使用一个任务完成了倒排索引及词频的排序任务，通过combine和reduce过程两次整合键值对，同时完成索引与排序任务，使得程序运行效率较高。

该程序对输入的作品数量没有限制，单词预处理（转换为小写，剔除停词，标点符号，数字等）、倒排索引和排序任务分别在map，combiner和reduce三个模块中实现，整体结构清晰，具有较好的可扩展性。

## 改进

改进思路与作业五的大致相同。对于标点符号的剔除，可以通过正则表达式完成：

```java
String token=token.replaceAll("[\\pP\\p{Punct}]", "");
```

通过该正则表达式可以提出字符串中的所有标点符号，只留下字母和数字。

另外，对于停词文件的读取，可以通过缓存加载来提高读取的速度：

在setup函数中读取停词

```java
URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
String patternsFileName = patternsPath.getName().toString();
parseSkipFile(patternsFileName);
```

```java
private void parseSkipFile(String fileName){
    try{
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while((pattern = fis.readLine()) != null){
            patternsToSkip.add(pattern);
        }
    }catch(IOException ioe){
        System.err.println("Caught exception while parsing the cached file '"
                          + StringUtils.stringifyException(ioe));
    }
}
```

在map函数中剔除单词中的停词：

```java
for(String pattern: patternsToSkip){
    line = line.replaceAll(pattern, "");
}
```

