package com.amqp.server;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class RPCServer {

  private static final String RPC_QUEUE_NAME = "rpc_queue";

  private static int sum(int n,int a) {
    return n+a;
  }

  public static void main(String[] argv) {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    Connection connection = null;
    try {
      connection      = factory.newConnection();
      final Channel channel = connection.createChannel();

      channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

      channel.basicQos(1);

      System.out.println(" [x] Awaiting RPC requests");

      Consumer consumer = new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                  .Builder()
                  .correlationId(properties.getCorrelationId())
                  .build();

          String response = "";

          try {
            String message = new String(body,"UTF-8");
            int[] numbers = Arrays.stream(message.split(",")).mapToInt(Integer::parseInt).toArray();
            int n = numbers[0];
            int a = numbers[1];
           // int n = Integer.parseInt(message);

            System.out.println(" [.] sum(" + message + ")");
            response += sum(n,a);
          }
          catch (RuntimeException e){
            System.out.println(" [.] " + e.toString());
          }
          finally {
            channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
            channel.basicAck(envelope.getDeliveryTag(), false);
            // RabbitMq consumer worker thread notifies the RPC server owner thread 
            synchronized(this) {
            	this.notify();
            }
          }
        }
      };

      channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
      // Wait and be prepared to consume the message from RPC client.
      while (true) {
      	synchronized(consumer) {
      		try {
      			consumer.wait();
      	    } catch (InterruptedException e) {
      	    	e.printStackTrace();	    	
      	    }
      	}
      }
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
    finally {
      if (connection != null)
        try {
          connection.close();
        } catch (IOException _ignore) {}
    }
  }
}