package com.alibaba.datax.plugin.writer.rediswriter.util;

import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import java.util.HashSet;
import java.util.Set;

public class RedisClusterUtil {


    public static RedisClusterConfiguration getClusterRedisConfig(Set<RedisNode> redisNodes){
        RedisClusterConfiguration jedisCluster = new RedisClusterConfiguration();
        jedisCluster.setMaxRedirects(3);
        jedisCluster.setClusterNodes(redisNodes);
        return jedisCluster;
    }

    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            Set<RedisNode> redisNodes,
            String password,
            int timeout
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(redisNodes));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(timeout);
        return factory;
    }

    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            Set<RedisNode> redisNodes,
            String password
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(redisNodes));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(3000);
        return factory;
    }


    public static RedisClusterConfiguration getClusterRedisConfig(String... hostsAndPorts ){

        Set<RedisNode> redisNodes = new HashSet<RedisNode>();
        redisNodes.clear();
        for (String hostAndPort : hostsAndPorts){
            String[] args = hostAndPort.split(":");
            if (args.length == 2) {
                RedisNode node = new RedisNode(args[0], Integer.valueOf(args[1]));
                redisNodes.add(node);
            }
        }
        RedisClusterConfiguration jedisCluster = new RedisClusterConfiguration();
        jedisCluster.setMaxRedirects(3);
        jedisCluster.setClusterNodes(redisNodes);
        return jedisCluster;
    }

    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            int timeout,String password,String... hostsAndPorts

    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(hostsAndPorts));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(timeout);
        return factory;
    }

    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            String password,String... hostsAndPorts
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(hostsAndPorts));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(3000);
        return factory;
    }


    public static RedisClusterConfiguration getClusterRedisConfig(String hostsAndPortsInOneString ){
        String[] hostsAndPorts = hostsAndPortsInOneString.split(",");
        Set<RedisNode> redisNodes = new HashSet<RedisNode>();
        redisNodes.clear();
        for (String hostAndPort : hostsAndPorts){
            String[] args = hostAndPort.split(":");
            if (args.length == 2) {
                RedisNode node = new RedisNode(args[0], Integer.valueOf(args[1]));
                redisNodes.add(node);
            }
        }
        RedisClusterConfiguration jedisCluster = new RedisClusterConfiguration();
        jedisCluster.setMaxRedirects(3);
        jedisCluster.setClusterNodes(redisNodes);
        return jedisCluster;
    }


    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            String hostsAndPortsInOneString,String password,int timeout

    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(hostsAndPortsInOneString));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(timeout);
        return factory;
    }

    public static JedisConnectionFactory clusterInstanceConnectionFactory(
            String hostsAndPortsInOneString,String password
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(getClusterRedisConfig(hostsAndPortsInOneString));
        factory.setPoolConfig(DefaultRedisPoolConfig.getInstance());
        factory.setPassword(password);
        factory.setTimeout(3000);
        return factory;
    }


    public static JedisConnectionFactory singleInstanceConnectionFactory(
            String hostName,
            String password,
            int port,
            int timeout
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory();
        factory.setHostName(hostName);
        factory.setPassword(password);
        factory.setPort(port);
        factory.setTimeout(timeout);
        return factory;
    }

    public static JedisConnectionFactory singleInstanceConnectionFactory(
            String hostName,
            String password,
            int port
    ){
        JedisConnectionFactory factory= new JedisConnectionFactory(
                DefaultRedisPoolConfig.getInstance()
        );
        factory.setHostName(hostName);
        factory.setPassword(password);
        factory.setPort(port);
        factory.setTimeout(3000);
        return factory;
    }


    public static JedisConnectionFactory singleInstanceConnectionFactory(
            String hostNameAndPort,
            String password
    ){
        String[] args = hostNameAndPort.split(":");
        JedisConnectionFactory factory= new JedisConnectionFactory();
        factory.setHostName(args[0]);
        System.out.println("hostname:" + args[0]);
        factory.setPassword(password);
        System.out.println("password:" + password);
        factory.setPort(Integer.valueOf(args[1]));
        System.out.println("port:" + args[1]);
        factory.setTimeout(3000);
        return factory;
    }


}
