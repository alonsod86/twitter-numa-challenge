package numa.executables;

import numa.core.Consumer;

/**
 * Created by alonso on 2/10/17.
 */
public class ElasticOutputApp {
    public static void main(String[] args) {
        Consumer consumer = new Consumer("kafka:9092");
        consumer.consume("localhost", 9200,"users", "tweets", "geo");
    }
}
