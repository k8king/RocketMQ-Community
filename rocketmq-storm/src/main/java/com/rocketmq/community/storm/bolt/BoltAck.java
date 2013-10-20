package com.rocketmq.community.storm.bolt;

import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BoltAck extends BaseRichBolt {
	
	private static final long serialVersionUID = -4651750180079307005L;
	private OutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {		
		System.out.println(input.getString(0));
		this._collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}