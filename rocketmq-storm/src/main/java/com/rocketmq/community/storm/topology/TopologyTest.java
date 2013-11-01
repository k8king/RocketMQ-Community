package com.rocketmq.community.storm.topology;

import com.rocketmq.community.storm.bolt.BoltAck;
import com.rocketmq.community.storm.scheme.StringScheme;
import com.rocketmq.community.storm.spout.RocketPushSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;

public class TopologyTest {

	public static void main(String[] args) {

		final Scheme scheme = new StringScheme();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("RocketPushSpout", new RocketPushSpout(scheme), 1);
		builder.setBolt("BoltAck", new BoltAck(), 2).shuffleGrouping("RocketPushSpout");
		Config conf = new Config();
		conf.put(RocketPushSpout.TOPIC, "kk");
		conf.put(RocketPushSpout.CONSUMERGROUP, "kk");
		conf.put(RocketPushSpout.NAMESERVER, "127.0.0.1:9876");

		try {
			if (args != null && args.length > 0) {
				conf.setNumWorkers(64);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} else {
				conf.setDebug(true);
				conf.setMaxTaskParallelism(1);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("test", conf, builder.createTopology());
				Thread.sleep(10000);
				cluster.shutdown();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}