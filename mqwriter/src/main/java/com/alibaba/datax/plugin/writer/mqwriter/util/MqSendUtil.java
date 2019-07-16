package com.alibaba.datax.plugin.writer.mqwriter.util;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MqSendUtil {

    private static Logger logger = LoggerFactory.getLogger(MqSendUtil.class);




    public static void rabbitMqProducer(String hostname, int port,
                                        String username, String password,
                                        String virtualHost, String queueName)
    {
        try
        {
            //获取连接
            Connection connection = MqReceiveUtil.getRabbitConnection(hostname, port, username, password, virtualHost);
            //从连接中获取一个通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(queueName, false, false, false, null);
            String message = "This is simple queue";
            //发送消息
            channel.basicPublish("", queueName, null, message.getBytes("utf-8"));
            System.out.println("[send]：" + message);
            channel.close();
            connection.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param producerGroup  produce1
     * @param hostAndPort  producerGroup
     * @param topic "TopicTest"
     * @param tags "Tag1"
     * @throws InterruptedException
     */
    public static void rocketMqProducer(String producerGroup, String hostAndPort,String topic,
                                        String tags) throws InterruptedException, MQClientException {

        //需要一个producer group名字作为构造方法的参数，这里为producer1
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        //设置NameServer地址,此处应改为实际NameServer地址，多个地址之间用；分隔
        //NameServer的地址必须有，但是也可以通过环境变量的方式设置，不一定非得写死在代码里
        producer.setNamesrvAddr(hostAndPort);
        producer.setVipChannelEnabled(false);

        //为避免程序启动的时候报错，添加此代码，可以让rocketMq自动创建topickey
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        producer.start();

        for(int i=0;i<10;i++){
            try {
                Message message = new Message(topic, tags,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = producer.send(message);

                System.out.println("发送的消息ID:" + sendResult.getMsgId() +"--- 发送消息的状态：" + sendResult.getSendStatus());
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();


    }


}
