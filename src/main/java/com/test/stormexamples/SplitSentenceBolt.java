package com.test.stormexamples;


import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

  private static final long serialVersionUID = 3092938699134129356L;
  
  private OutputCollector collector;
  
  //The prepare method provides the bolt with an OutputCollector that is used for emitting tuples from this bolt. 
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    collector = outCollector;
    System.out.println("SplitSentenceBolt : prepare "+cfg);
  }

  //The declareOutputFields method declares that the Bolt emits 1-tuples with one field called "word".
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
    System.out.println("SplitSentenceBolt : declare "+declarer);
  }

  //The execute method receives a tuple from one of the bolt's inputs or spouts. 
  @Override
    public void execute(Tuple tuple) {
      
    Object value = tuple.getValue(0);
    String sentence = null;
    
    System.out.println("SplitSentenceBolt : execute "+value + "-- Value is "+tuple);
    
    if (value instanceof String) {
      sentence = (String) value;

    } else {
      // Kafka returns bytes
      byte[] bytes = (byte[]) value;
      try {
        sentence = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }      
    }

    String[] words = sentence.split("\\s+");
    System.out.println("SplitSentenceBolt : execute sentence is "+sentence);
    
    for (String word : words) {
        System.out.println("SplitSentenceBolt : execute words : "+word);
        
        if(word.equals("error")){
            
            collector.emit(tuple, new Values(word));
        }
            
    }
    collector.ack(tuple);
  }
}