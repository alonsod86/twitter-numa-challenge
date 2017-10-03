package numa.executables;

import com.google.gson.Gson;
import numa.core.Producer;
import twitter4j.*;

/**
 * Created by alonso on 2/10/17.
 */
public class TwitterStreamApp {

    private static Producer producer = new Producer("kafka:9092");

    /**
     * Main entry of this application.
     *
     * @param args arguments doesn't take effect with this example
     * @throws TwitterException when Twitter service or network is unavailable
     */
    public static void main(String[] args) throws TwitterException {
        Gson gson = new Gson();
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        StatusListener listener = new StatusListener() {

            /**
             * Asynchronous tweet reception
             * @param status tweet content
             */
            @Override
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                // Users splitter
                producer.send("users", gson.toJson(status.getUser()));
                // Tweets splitter
                producer.send("tweets", gson.toJson(status));
                // Geolocation splitter + enrichment for elastic
                if (status.getGeoLocation()!=null)
                    producer.send("geo", "{\"coordinates\": {" +
                            "\"lat\":"+status.getGeoLocation().getLatitude()+
                            ",\"lon\":"+status.getGeoLocation().getLongitude()+"}" +
                            "}");
            }

            /**
             * Tweet deletion notification
             * @param statusDeletionNotice
             */
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            /**
             * Slow client reception notifications
             * @param numberOfLimitedStatuses
             */
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
