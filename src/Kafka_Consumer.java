
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.sql.*;

import java.util.Arrays;
import java.util.Properties;

public class Kafka_Consumer {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;

        KafkaConsumer consumer;
        Properties props=new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id","test");

        consumer =new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("TempMonitoringSystem")); //pass the topic name as the parameter

        while (true){
            ConsumerRecords<String,String> records=consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn=(Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/iot_db","root","");
                    System.out.println("Connection created successfully");

                    stmt=(Statement) conn.createStatement();

                    String fetchValue=record.value();
                    System.out.println(fetchValue);

                    JSONObject jsonObject = new JSONObject(fetchValue);
                    String getTemp = String.valueOf(jsonObject.getInt("temp"));
                    String getHum = String.valueOf(jsonObject.getInt("humid"));

                    String query = "INSERT INTO `temperature`(`Temperature`, `Humidity`) VALUES ("+getTemp+","+getHum+")";

                    stmt.executeUpdate(query);
                    System.out.println("value inserted successfully");

                }
                catch (Exception e){
                    System.out.println(e);
                }
                System.out.println(record.value());
            }

        }



    }
}
