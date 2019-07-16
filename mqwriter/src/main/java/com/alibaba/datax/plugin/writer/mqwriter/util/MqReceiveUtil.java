package com.alibaba.datax.plugin.writer.mqwriter.util;

import com.rabbitmq.client.*;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class MqReceiveUtil {

    private static Logger logger = LoggerFactory.getLogger(MqReceiveUtil.class);

    public static Connection getRabbitConnection(String hostname, int port,String username, String password, String virtualHost )
    {
        try
        {
            Connection connection = null;
            //定义一个连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            //设置服务端地址（域名地址/ip）
            factory.setHost(hostname);
            //设置服务器端口号
            factory.setPort(port);
            //设置虚拟主机(相当于数据库中的库)
            factory.setVirtualHost(virtualHost);
            //设置用户名
            factory.setUsername(username);
            //设置密码
            factory.setPassword(password);
            connection = factory.newConnection();
            return connection;
        }
        catch (Exception e)
        {
            return null;
        }
    }


    public static void rabbitMqConsumer(String hostname, int port,
                                        String username, String password,
                                        String virtualHost, String queueName)
    {
        try
        {
            //获取连接
            Connection connection = getRabbitConnection(hostname, port, username, password, virtualHost);
            //从连接中获取一个通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(queueName, false, false, false, null);
            //定义消费者
            DefaultConsumer consumer = new DefaultConsumer(channel)
            {
                //当消息到达时执行回调方法
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                                           byte[] body) throws IOException
                {
                    String message = new String(body, "utf-8");
                    System.out.println("[Receive]：" + message);
                }
            };
            //监听队列
            channel.basicConsume(queueName, true, consumer);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public static void rocketMqConsumer(String consumerGroup, String hostAndPort,String topic) throws MQClientException {

        rocketMqConsumer( consumerGroup,  hostAndPort, topic, "*");

    }

    public static void rocketMqConsumer(String consumerGroup, String hostAndPort,String topic,
                                        String pattern) throws MQClientException {

        //设置消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setVipChannelEnabled(false);
        consumer.setNamesrvAddr(hostAndPort);
        //设置消费者端消息拉取策略，表示从哪里开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消费者拉取消息的策略，*表示消费该topic下的所有消息，也可以指定tag进行消息过滤
        consumer.subscribe(topic, pattern);

        //消费者端启动消息监听，一旦生产者发送消息被监听到，就打印消息，和rabbitmq中的handlerDelivery类似
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : msgs) {
                    String topic = messageExt.getTopic();
                    String tag = messageExt.getTags();
                    String msg = new String(messageExt.getBody());
                    System.out.println("*********************************");
                    System.out.println("消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + msg + ", tag:" + tag + ", topic:" + topic);
                    System.out.println("*********************************");
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //调用start()方法启动consumer
        consumer.start();
        logger.info("Consumer Started....");


    }

}