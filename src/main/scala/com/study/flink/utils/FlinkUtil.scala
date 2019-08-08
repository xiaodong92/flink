package com.study.flink.utils

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.plain.PlainSaslServer

object FlinkUtil {

    private val OFFSET_RESET_LATEST = "latest"
    private val FETCH_MIN_BYTES = "4096"
    private val ENABLE_AUTO_COMMIT = "false"
    private val PARTITION_DISCOVERY_MILLIS = "10000"
    private val SASL_JAAS =
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"

    def getEnv(params: ParameterTool, parallelism: Int = 0,
               stateBackendPath: String = ""): StreamExecutionEnvironment = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //禁用sys日志
        env.getConfig.disableSysoutLogging()
        //检查点时间间隔
        env.enableCheckpointing(30000)
        //下一次执行checkpoint的最小停顿时间
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
        //设置检查点在程序停止后不要自动清除
        env.getCheckpointConfig
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        //设置state的存储路径
        if (StringUtils.isNotBlank(stateBackendPath)) {
            env.setStateBackend(new FsStateBackend(stateBackendPath, true).asInstanceOf[StateBackend])
        }
        //程序失败后重启间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10000))
        env.getConfig.setGlobalJobParameters(params)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        //设置程序计算时间为
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        if (0 != parallelism) {
            env.setParallelism(parallelism)
        }
        env
    }

    def getKafkaConsumerProp(servers: String, groupId: String): Properties = {
        getKafkaConsumerProp(servers, groupId, enableSecurity = false, null, null)
    }

    def getKafkaConsumerProp(servers: String, groupId: String, enableSecurity: Boolean, username: String,
                             password: String): Properties = {
        val properties = new Properties()
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_LATEST)
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTES)
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT)
        properties.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_MILLIS)
        if (enableSecurity) {
            properties.put(SaslConfigs.SASL_MECHANISM, PlainSaslServer.PLAIN_MECHANISM)
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, SASL_JAAS.format(username, password))
        }
        properties
    }
}
