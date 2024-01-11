package kafka.examples;

public class CreateTopics {

    public static void main(String[] args) {
        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 1, "test", "output");
    }
}
