package com.example.rabbitmqDemo.Taskdistribution;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by shichong on 2018/9/13.
 */
public class Work1 {
    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        final ConnectionFactory factory =new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel =connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME,true,false,false,null);
        System.out.println("Work1 Waiting for message");

        //每次从队列获取数量
        channel.basicQos(1);

        final Consumer consumer =new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body,"UTF-8");
                System.out.println("Work1 Received '"+message+"'");
                try {
                    throw new Exception();
                } catch (Exception e) {
                    channel.abort();
                }finally {
                    System.out.println("Work1 Done");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        boolean autoAck = false;
        //消息消费完成确认
        channel.basicConsume(TASK_QUEUE_NAME,autoAck,consumer);
    }

}
