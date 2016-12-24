package com.test.stormexamples;



import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.topology.TopologyBuilder;


import org.apache.storm.tuple.Fields;

public class WordCountAckedTopologyRemote {

  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
  private static final String SPLIT_BOLT_ID = "acking-split-bolt";
  private static final String COUNT_BOLT_ID = "acking-count-bolt";
  private static final String REPORT_BOLT_ID = "acking-report-bolt";
  private static final String TOPOLOGY_NAME = "ErrorCount";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;
    
    KafkaSpout kspout = buildKafkaSentenceSpout();
    SplitSentenceBolt splitBolt = new SplitSentenceBolt();
    WordCountBolt countBolt = new WordCountBolt();
    
    
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
    builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
    
    
    Config cfg = new Config();  
    cfg.setDebug(true);
    //cfg.setNumWorkers(numSpoutExecutors);
     
    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
    
    
    //Map conf = Utils.readStormConfig();
    //Client client = NimbusClient.getConfiguredClient(conf).getClient();
    //KillOptions killOpts = new KillOptions();
    //killOpts.set_wait_secs(120); // time to wait before killing
    //client.killTopologyWithOpts(TOPOLOGY_NAME, killOpts); //provide topology name
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "localhost:19100";
    String topic = "testTopic";

    String zkRoot = "/storm";
    String zkSpoutId = "acking-sentence-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}