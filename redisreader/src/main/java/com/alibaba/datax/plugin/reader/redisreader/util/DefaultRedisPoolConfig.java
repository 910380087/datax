package com.alibaba.datax.plugin.reader.redisreader.util;

import redis.clients.jedis.JedisPoolConfig;

public class DefaultRedisPoolConfig extends JedisPoolConfig{

    private JedisPoolConfig defaultRedisPoolConfig = new JedisPoolConfig();


    public JedisPoolConfig getDefaultRedisPoolConfig() {
        return defaultRedisPoolConfig;
    }

    private DefaultRedisPoolConfig() {

        defaultRedisPoolConfig.setMaxTotal(20);
//        <!-- 最大连接数 -->
        defaultRedisPoolConfig.setMaxTotal(20);
//        <!-- 最大空闲连接数 -->
        defaultRedisPoolConfig.setMaxIdle(10);
//        <!-- 每次释放连接的最大数目 -->
        defaultRedisPoolConfig.setNumTestsPerEvictionRun(1024);
//        <!-- 释放连接的扫描间隔（毫秒） -->
        defaultRedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000);
//        <!-- 连接最小空闲时间 -->
        defaultRedisPoolConfig.setMinEvictableIdleTimeMillis(1800000);
//        <!-- 连接空闲多久后释放, 当空闲时间>该值 且 空闲连接>最大空闲连接数 时直接释放 -->
        defaultRedisPoolConfig.setSoftMinEvictableIdleTimeMillis(10000);
//        <!-- 获取连接时的最大等待毫秒数,小于零:阻塞不确定的时间,默认-1 -->
        defaultRedisPoolConfig.setMaxWaitMillis(1500);
//        <!-- 在获取连接的时候检查有效性, 默认false -->
        defaultRedisPoolConfig.setTestOnBorrow(true);
//        <!-- 在空闲时检查有效性, 默认false -->
        defaultRedisPoolConfig.setTestWhileIdle(true);
//        <!-- 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true -->
        defaultRedisPoolConfig.setBlockWhenExhausted(false);
    }

    public static JedisPoolConfig getInstance(){
        DefaultRedisPoolConfig defaultRedisPoolConfig = new DefaultRedisPoolConfig();
         return defaultRedisPoolConfig.getDefaultRedisPoolConfig();
    }



}
