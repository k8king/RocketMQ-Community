package com.rocketmq.community.storm.spout;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedTransferQueue;


import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class RocketPushSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3114920454252653207L;
	
	public static final String TOPIC = "meta.Topic";
	public static final String CONSUMERGROUP = "meta.ConsumerGroup";
	public static final String NAMESERVER = "meta.NameServer";
	
	private transient ConcurrentHashMap<String, List<MessageExt>> id2wrapperMap;
	private transient SpoutOutputCollector collector;
	private  LinkedTransferQueue<List<MessageExt>> messageQueue;
	private DefaultMQPushConsumer consumer;
	/**
	* Time in milliseconds to wait for a message from the queue if there is no
	* message ready when the topology requests a tuple (via
	* {@link #nextTuple()}).
	*/
	private static final long WAIT_FOR_NEXT_MESSAGE = 1L;
	private final Scheme scheme;
	static final Log log = LogFactory.getLog(RocketPushSpout.class);
		
	public RocketPushSpout(final Scheme scheme) {
		super();
		this.scheme = scheme;
		
	}

	@Override
	@SuppressWarnings("rawtypes")	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {		
		final String consumerGroup = (String) conf.get(CONSUMERGROUP);
		final String nameServer = (String) conf.get(NAMESERVER);
		final String topic = (String) conf.get(TOPIC);
        if (topic == null || consumerGroup == null || nameServer == null) {
            throw new IllegalArgumentException(TOPIC +" is "+topic+"\n"
            		+CONSUMERGROUP +" is "+consumerGroup+"\n"
            			+NAMESERVER +" is "+nameServer+"\n" );
        }
        this.consumer = new DefaultMQPushConsumer();
        this.id2wrapperMap = new ConcurrentHashMap<String, List<MessageExt>>();
        this.messageQueue = new LinkedTransferQueue<List<MessageExt>>();
        try {
            this.collector = collector;
            this.setUpMeta(consumerGroup,nameServer,topic);
        }
        catch (final MQClientException e) {
            log.error("Setup meta consumer failed", e);
        }
		
	}
	
	private void setUpMeta(final String consumerGroup, final String nameServer,final String topic) throws MQClientException {
		log.info(String.format("initialize consumer..."));		
		this.consumer.setConsumerGroup(consumerGroup);
		this.consumer.setInstanceName(UUID.randomUUID().toString());
		this.consumer.setNamesrvAddr(nameServer);
		this.consumer.subscribe(topic,"*");	
		this.consumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
        	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                 RocketPushSpout.this.id2wrapperMap.put(msgs.get(0).getMsgId(), msgs);
                 RocketPushSpout.this.messageQueue.offer(msgs);                 
                 return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        	}
        });
        consumer.start();
    }
	
    @Override
    public void close() {
    	this.consumer.shutdown();
    }
	
	@Override
	public void nextTuple() {

		if (this.consumer != null) {
			try {
				final List<MessageExt> msgs = this.messageQueue.poll(WAIT_FOR_NEXT_MESSAGE, TimeUnit.MILLISECONDS);
                if (msgs == null) {
                	return;
                }
                this.collector.emit(scheme.deserialize(msgs.get(0).getBody()), msgs.get(0).getMsgId());
            } catch (final InterruptedException e) {
                // interrupted while waiting for message, big deal
            }
        }	
	}
	
    @Override
    public void ack(final Object msgId) {
        if (msgId instanceof String) {
            final String id = (String) msgId;
            final List<MessageExt> msgs = this.id2wrapperMap.remove(id);
            
            if (msgs == null) {
                log.warn(String.format("don't know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
                return;
            }
        }
        else {
            log.warn(String.format("don't know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
        }
    }
    
    @Override
    public void fail(final Object msgId) {
        if (msgId instanceof String) {
            final String id = (String) msgId;
            final List<MessageExt> msgs = this.id2wrapperMap.remove(id);
            if (msgs == null) {
                log.warn(String.format("don't know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
                return;
            }
        }
        else {
            log.warn(String.format("don't know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
        }
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.scheme.getOutputFields());
	}
	
}