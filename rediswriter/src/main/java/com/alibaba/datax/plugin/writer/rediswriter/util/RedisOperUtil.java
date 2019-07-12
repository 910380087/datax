package com.alibaba.datax.plugin.writer.rediswriter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

public class RedisOperUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RedisOperUtil.class);

    public static RedisUtil getSigleNodeRedisUtil(String hostNameAndPort,
                                                  String password){
        LOG.info("初始化Writer单例模式RedisTemplate");
        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
        LOG.info("设置Writer单例模式RedisTemplate的Connection");
        stringRedisTemplate.setConnectionFactory(RedisClusterUtil.singleInstanceConnectionFactory(
                hostNameAndPort,password
        ));
        LOG.info("设置Writer单例模式RedisTemplate的序列化参数");
        stringRedisTemplate.setKeySerializer(new StringRedisSerializer());
        stringRedisTemplate.setValueSerializer(new StringRedisSerializer());
        stringRedisTemplate.setHashKeySerializer(new StringRedisSerializer());
        stringRedisTemplate.setHashValueSerializer(new StringRedisSerializer());
        stringRedisTemplate.setEnableTransactionSupport(true);
        RedisUtil redisUtil = new RedisUtil(stringRedisTemplate);
        return redisUtil;
    }

    public static  RedisUtil getSigleNodeRedisUtil(String hostName,
                                                   String password,
                                                   int port){

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
        stringRedisTemplate.setConnectionFactory(RedisClusterUtil.singleInstanceConnectionFactory(
                hostName,password,port
        ));
        stringRedisTemplate.setKeySerializer(new StringRedisSerializer());
        stringRedisTemplate.setEnableTransactionSupport(true);
        RedisUtil redisUtil = new RedisUtil(stringRedisTemplate);
        return redisUtil;
    }



    public static RedisUtil getClusterNodesRedisUtil(String hostNamesAndPorts,
                                                     String password){
        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
        stringRedisTemplate.setConnectionFactory(RedisClusterUtil.clusterInstanceConnectionFactory(
                hostNamesAndPorts,password
        ));
        stringRedisTemplate.setKeySerializer(new StringRedisSerializer());
        stringRedisTemplate.setEnableTransactionSupport(true);
        RedisUtil redisUtil = new RedisUtil(stringRedisTemplate);
        return redisUtil;
    }


}

