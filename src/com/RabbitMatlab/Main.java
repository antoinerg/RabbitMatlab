package com.RabbitMatlab;

import matlabcontrol.*; // MatlabControl classes
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class Main {

    private final static String JOB_QUEUE_NAME = "matlab";
	private final static String FAILED_JOB_QUEUE_NAME = "matlab_failed";

    public static void main(String[] argv) throws Exception {
		String amqp_uri = argv[0];
		// Connect to RabbitMQ
    	ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(amqp_uri);
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();
		
		// Only fetch one message at a time to ensure fair dispatching
		int prefetchCount = 1;
		channel.basicQos(prefetchCount);

		// Declare the job queue
	    channel.queueDeclare(JOB_QUEUE_NAME, false, false, false, null);
	
		// Declare the failed job queue
		channel.queueDeclare(FAILED_JOB_QUEUE_NAME,false,false,false,null);
	
	    System.out.println(" [*] Waiting for jobs. To exit press CTRL+C");
    
		// Start consuming
	    QueueingConsumer consumer = new QueueingConsumer(channel);
		boolean autoAck = false;
	    channel.basicConsume(JOB_QUEUE_NAME, autoAck, consumer);
	
		//Create a proxy, which we will use to control MATLAB
	    MatlabProxyFactory matlab_factory = new MatlabProxyFactory();
	    MatlabProxy proxy = matlab_factory.getProxy();
    
	    while (true) {
	      QueueingConsumer.Delivery delivery = consumer.nextDelivery();	
		  try
		  {
		      String message = new String(delivery.getBody());
			  // Clear everything close all the file to start fresh
		      proxy.eval("close all;clear all;fclose all");
			  // Fire off command
			  proxy.eval(message);
			  // Acknowledge we have received and processed the message
			  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		      System.out.println(" [x] Received '" + message + "'");
		  }
		  catch (MatlabInvocationException e)
		  {
			  // Publish on failed job queue
			  channel.basicPublish("", FAILED_JOB_QUEUE_NAME, null, delivery.getBody());
			  // Acknowledge
			  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		  }
	    }
 	}
}