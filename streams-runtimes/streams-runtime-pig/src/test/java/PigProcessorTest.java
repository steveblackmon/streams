import org.apache.pig.pigunit.PigTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by sblackmon on 3/30/14.
 */
public class PigProcessorTest {

    @Ignore
    @Test
    public void testPigProcessor() throws Exception {
        String[] args = {};

        String[] input = {
                "159475541894897679\ttwitter,statuses/user_timeline\t1384499359006\t{\"id\":\"id:twitter:share:159475541894897679\",\"actor\":{\"id\":\"id:twitter:27552112\",\"displayName\":\"rmedinaflores\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"verb\":\"share\",\"object\":{\"id\":\"159470076259602432\",\"objectType\":\"tweet\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"published\":{\"year\":2012,\"era\":1,\"dayOfMonth\":17,\"dayOfWeek\":2,\"dayOfYear\":17,\"weekOfWeekyear\":3,\"weekyear\":2012,\"monthOfYear\":1,\"yearOfEra\":2012,\"yearOfCentury\":12,\"centuryOfEra\":20,\"millisOfSecond\":0,\"millisOfDay\":69706000,\"secondOfMinute\":46,\"secondOfDay\":69706,\"minuteOfHour\":21,\"minuteOfDay\":1161,\"hourOfDay\":19,\"zone\":{\"fixed\":false,\"uncachedZone\":{\"cachable\":true,\"fixed\":false,\"id\":\"America/Los_Angeles\"},\"id\":\"America/Los_Angeles\"},\"millis\":1326856906000,\"chronology\":{\"zone\":{\"fixed\":false,\"uncachedZone\":{\"cachable\":true,\"fixed\":false,\"id\":\"America/Los_Angeles\"},\"id\":\"America/Los_Angeles\"}},\"afterNow\":false,\"beforeNow\":true,\"equalNow\":false},\"provider\":{\"id\":\"id:providers:twitter\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"title\":\"\",\"content\":\"The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"url\":\"http://twitter.com/159475541894897679\",\"links\":[\"http://ti.me/zYyEtD\"],\"extensions\":{\"twitter\":{\"retweeted_status\":{\"text\":\"The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"retweeted\":false,\"truncated\":false,\"entities\":{\"user_mentions\":[{\"id\":245888431,\"name\":\"TIME Moneyland\",\"indices\":[106,120],\"screen_name\":\"TIMEMoneyland\",\"id_str\":\"245888431\",\"additionalProperties\":{}}],\"hashtags\":[],\"urls\":[{\"expanded_url\":\"http://ti.me/zYyEtD\",\"indices\":[80,100],\"display_url\":\"ti.me/zYyEtD\",\"url\":\"http://t.co/M9UUNvZi\",\"additionalProperties\":{}}],\"additionalProperties\":{\"symbols\":[]},\"symbols\":[]},\"id\":159470076259602432,\"source\":\"<a href=\\\"http://www.hootsuite.com\\\" rel=\\\"nofollow\\\">HootSuite</a>\",\"lang\":\"en\",\"favorited\":false,\"possibly_sensitive\":false,\"created_at\":\"20120117T190003.000-0800\",\"retweet_count\":71,\"id_str\":\"159470076259602432\",\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":70754,\"profile_background_tile\":true,\"lang\":\"en\",\"profile_link_color\":\"1B4F89\",\"id\":14293310,\"protected\":false,\"favourites_count\":59,\"profile_text_color\":\"000000\",\"verified\":true,\"description\":\"Breaking news and current events from around the globe. Hosted by TIME staff. Tweet questions to our customer service team @TIMEmag_Service.\",\"contributors_enabled\":false,\"name\":\"TIME.com\",\"profile_sidebar_border_color\":\"000000\",\"profile_background_color\":\"CC0000\",\"created_at\":\"20080403T065430.000-0700\",\"default_profile_image\":false,\"followers_count\":5146268,\"geo_enabled\":false,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/1700796190/Picture_24_normal.png\",\"profile_background_image_url\":\"http://a0.twimg.com/profile_background_images/735228291/107f1a300a90ee713937234bb3d139c0.jpeg\",\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/735228291/107f1a300a90ee713937234bb3d139c0.jpeg\",\"follow_request_sent\":false,\"url\":\"http://t.co/4aYbUuAeSh\",\"utc_offset\":-18000,\"time_zone\":\"Eastern Time (US & Canada)\",\"profile_use_background_image\":true,\"friends_count\":742,\"profile_sidebar_fill_color\":\"D9D9D9\",\"screen_name\":\"TIME\",\"id_str\":\"14293310\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/1700796190/Picture_24_normal.png\",\"is_translator\":false,\"listed_count\":76944,\"additionalProperties\":{\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]},\"url\":{\"urls\":[{\"expanded_url\":\"http://www.time.com\",\"indices\":[0,22],\"display_url\":\"time.com\",\"url\":\"http://t.co/4aYbUuAeSh\"}]}},\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/14293310/1355243462\"},\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]},\"url\":{\"urls\":[{\"expanded_url\":\"http://www.time.com\",\"indices\":[0,22],\"display_url\":\"time.com\",\"url\":\"http://t.co/4aYbUuAeSh\"}]}},\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/14293310/1355243462\"},\"additionalProperties\":{\"geo\":null,\"favorite_count\":14,\"place\":null},\"geo\":null,\"favorite_count\":14,\"place\":null},\"additionalProperties\":{\"geo\":null,\"favorite_count\":0,\"place\":null},\"text\":\"RT @TIME: The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"retweeted\":false,\"truncated\":false,\"entities\":{\"user_mentions\":[{\"id\":14293310,\"name\":\"TIME.com\",\"indices\":[3,8],\"screen_name\":\"TIME\",\"id_str\":\"14293310\",\"additionalProperties\":{}},{\"id\":245888431,\"name\":\"TIME Moneyland\",\"indices\":[116,130],\"screen_name\":\"TIMEMoneyland\",\"id_str\":\"245888431\",\"additionalProperties\":{}}],\"hashtags\":[],\"urls\":[{\"expanded_url\":\"http://ti.me/zYyEtD\",\"indices\":[90,110],\"display_url\":\"ti.me/zYyEtD\",\"url\":\"http://t.co/M9UUNvZi\",\"additionalProperties\":{}}],\"additionalProperties\":{\"symbols\":[]},\"symbols\":[]},\"id\":159475541894897679,\"source\":\"<a href=\\\"http://twitter.com/download/iphone\\\" rel=\\\"nofollow\\\">Twitter for iPhone</a>\",\"lang\":\"en\",\"favorited\":false,\"possibly_sensitive\":false,\"created_at\":\"20120117T192146.000-0800\",\"retweet_count\":71,\"id_str\":\"159475541894897679\",\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":5053,\"profile_background_tile\":true,\"lang\":\"en\",\"profile_link_color\":\"738D84\",\"id\":27552112,\"protected\":false,\"favourites_count\":52,\"profile_text_color\":\"97CEC9\",\"verified\":false,\"description\":\"\",\"contributors_enabled\":false,\"name\":\"rafael medina-flores\",\"profile_sidebar_border_color\":\"A9AC00\",\"profile_background_color\":\"C5EFE3\",\"created_at\":\"20090329T182155.000-0700\",\"default_profile_image\":false,\"followers_count\":963,\"geo_enabled\":true,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/2519547938/image_normal.jpg\",\"profile_background_image_url\":\"http://a0.twimg.com/profile_background_images/167479660/trireme.jpg\",\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/167479660/trireme.jpg\",\"follow_request_sent\":false,\"utc_offset\":-25200,\"time_zone\":\"Mountain Time (US & Canada)\",\"profile_use_background_image\":true,\"friends_count\":1800,\"profile_sidebar_fill_color\":\"5C4F3C\",\"screen_name\":\"rmedinaflores\",\"id_str\":\"27552112\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/2519547938/image_normal.jpg\",\"is_translator\":false,\"listed_count\":50,\"additionalProperties\":{\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]}}},\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]}}},\"geo\":null,\"favorite_count\":0,\"place\":null},\"location\":{\"id\":\"id:twitter:159475541894897679\",\"coordinates\":null}}}",
        };

        String[] output = {
                "159475541894897679\ttwitter,statuses/user_timeline\t1384499359006\t{\"id\":\"id:twitter:share:159475541894897679\",\"actor\":{\"id\":\"id:twitter:27552112\",\"displayName\":\"rmedinaflores\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"verb\":\"share\",\"object\":{\"id\":\"159470076259602432\",\"objectType\":\"tweet\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"published\":{\"year\":2012,\"era\":1,\"dayOfMonth\":17,\"dayOfWeek\":2,\"dayOfYear\":17,\"weekOfWeekyear\":3,\"weekyear\":2012,\"monthOfYear\":1,\"yearOfEra\":2012,\"yearOfCentury\":12,\"centuryOfEra\":20,\"millisOfSecond\":0,\"millisOfDay\":69706000,\"secondOfMinute\":46,\"secondOfDay\":69706,\"minuteOfHour\":21,\"minuteOfDay\":1161,\"hourOfDay\":19,\"zone\":{\"fixed\":false,\"uncachedZone\":{\"cachable\":true,\"fixed\":false,\"id\":\"America/Los_Angeles\"},\"id\":\"America/Los_Angeles\"},\"millis\":1326856906000,\"chronology\":{\"zone\":{\"fixed\":false,\"uncachedZone\":{\"cachable\":true,\"fixed\":false,\"id\":\"America/Los_Angeles\"},\"id\":\"America/Los_Angeles\"}},\"afterNow\":false,\"beforeNow\":true,\"equalNow\":false},\"provider\":{\"id\":\"id:providers:twitter\",\"attachments\":[],\"upstreamDuplicates\":[],\"downstreamDuplicates\":[]},\"title\":\"\",\"content\":\"The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"url\":\"http://twitter.com/159475541894897679\",\"links\":[\"http://ti.me/zYyEtD\"],\"extensions\":{\"twitter\":{\"retweeted_status\":{\"text\":\"The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"retweeted\":false,\"truncated\":false,\"entities\":{\"user_mentions\":[{\"id\":245888431,\"name\":\"TIME Moneyland\",\"indices\":[106,120],\"screen_name\":\"TIMEMoneyland\",\"id_str\":\"245888431\",\"additionalProperties\":{}}],\"hashtags\":[],\"urls\":[{\"expanded_url\":\"http://ti.me/zYyEtD\",\"indices\":[80,100],\"display_url\":\"ti.me/zYyEtD\",\"url\":\"http://t.co/M9UUNvZi\",\"additionalProperties\":{}}],\"additionalProperties\":{\"symbols\":[]},\"symbols\":[]},\"id\":159470076259602432,\"source\":\"<a href=\\\"http://www.hootsuite.com\\\" rel=\\\"nofollow\\\">HootSuite</a>\",\"lang\":\"en\",\"favorited\":false,\"possibly_sensitive\":false,\"created_at\":\"20120117T190003.000-0800\",\"retweet_count\":71,\"id_str\":\"159470076259602432\",\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":70754,\"profile_background_tile\":true,\"lang\":\"en\",\"profile_link_color\":\"1B4F89\",\"id\":14293310,\"protected\":false,\"favourites_count\":59,\"profile_text_color\":\"000000\",\"verified\":true,\"description\":\"Breaking news and current events from around the globe. Hosted by TIME staff. Tweet questions to our customer service team @TIMEmag_Service.\",\"contributors_enabled\":false,\"name\":\"TIME.com\",\"profile_sidebar_border_color\":\"000000\",\"profile_background_color\":\"CC0000\",\"created_at\":\"20080403T065430.000-0700\",\"default_profile_image\":false,\"followers_count\":5146268,\"geo_enabled\":false,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/1700796190/Picture_24_normal.png\",\"profile_background_image_url\":\"http://a0.twimg.com/profile_background_images/735228291/107f1a300a90ee713937234bb3d139c0.jpeg\",\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/735228291/107f1a300a90ee713937234bb3d139c0.jpeg\",\"follow_request_sent\":false,\"url\":\"http://t.co/4aYbUuAeSh\",\"utc_offset\":-18000,\"time_zone\":\"Eastern Time (US & Canada)\",\"profile_use_background_image\":true,\"friends_count\":742,\"profile_sidebar_fill_color\":\"D9D9D9\",\"screen_name\":\"TIME\",\"id_str\":\"14293310\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/1700796190/Picture_24_normal.png\",\"is_translator\":false,\"listed_count\":76944,\"additionalProperties\":{\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]},\"url\":{\"urls\":[{\"expanded_url\":\"http://www.time.com\",\"indices\":[0,22],\"display_url\":\"time.com\",\"url\":\"http://t.co/4aYbUuAeSh\"}]}},\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/14293310/1355243462\"},\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]},\"url\":{\"urls\":[{\"expanded_url\":\"http://www.time.com\",\"indices\":[0,22],\"display_url\":\"time.com\",\"url\":\"http://t.co/4aYbUuAeSh\"}]}},\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/14293310/1355243462\"},\"additionalProperties\":{\"geo\":null,\"favorite_count\":14,\"place\":null},\"geo\":null,\"favorite_count\":14,\"place\":null},\"additionalProperties\":{\"geo\":null,\"favorite_count\":0,\"place\":null},\"text\":\"RT @TIME: The Costa Concordia cruise ship accident could be a disaster for the industry | http://t.co/M9UUNvZi (via @TIMEMoneyland)\",\"retweeted\":false,\"truncated\":false,\"entities\":{\"user_mentions\":[{\"id\":14293310,\"name\":\"TIME.com\",\"indices\":[3,8],\"screen_name\":\"TIME\",\"id_str\":\"14293310\",\"additionalProperties\":{}},{\"id\":245888431,\"name\":\"TIME Moneyland\",\"indices\":[116,130],\"screen_name\":\"TIMEMoneyland\",\"id_str\":\"245888431\",\"additionalProperties\":{}}],\"hashtags\":[],\"urls\":[{\"expanded_url\":\"http://ti.me/zYyEtD\",\"indices\":[90,110],\"display_url\":\"ti.me/zYyEtD\",\"url\":\"http://t.co/M9UUNvZi\",\"additionalProperties\":{}}],\"additionalProperties\":{\"symbols\":[]},\"symbols\":[]},\"id\":159475541894897679,\"source\":\"<a href=\\\"http://twitter.com/download/iphone\\\" rel=\\\"nofollow\\\">Twitter for iPhone</a>\",\"lang\":\"en\",\"favorited\":false,\"possibly_sensitive\":false,\"created_at\":\"20120117T192146.000-0800\",\"retweet_count\":71,\"id_str\":\"159475541894897679\",\"user\":{\"location\":\"\",\"default_profile\":false,\"statuses_count\":5053,\"profile_background_tile\":true,\"lang\":\"en\",\"profile_link_color\":\"738D84\",\"id\":27552112,\"protected\":false,\"favourites_count\":52,\"profile_text_color\":\"97CEC9\",\"verified\":false,\"description\":\"\",\"contributors_enabled\":false,\"name\":\"rafael medina-flores\",\"profile_sidebar_border_color\":\"A9AC00\",\"profile_background_color\":\"C5EFE3\",\"created_at\":\"20090329T182155.000-0700\",\"default_profile_image\":false,\"followers_count\":963,\"geo_enabled\":true,\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/2519547938/image_normal.jpg\",\"profile_background_image_url\":\"http://a0.twimg.com/profile_background_images/167479660/trireme.jpg\",\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/167479660/trireme.jpg\",\"follow_request_sent\":false,\"utc_offset\":-25200,\"time_zone\":\"Mountain Time (US & Canada)\",\"profile_use_background_image\":true,\"friends_count\":1800,\"profile_sidebar_fill_color\":\"5C4F3C\",\"screen_name\":\"rmedinaflores\",\"id_str\":\"27552112\",\"profile_image_url\":\"http://pbs.twimg.com/profile_images/2519547938/image_normal.jpg\",\"is_translator\":false,\"listed_count\":50,\"additionalProperties\":{\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]}}},\"following\":false,\"notifications\":false,\"entities\":{\"description\":{\"urls\":[]}}},\"geo\":null,\"favorite_count\":0,\"place\":null},\"location\":{\"id\":\"id:twitter:159475541894897679\",\"coordinates\":null}}}",
        };

        PigTest test;
        test = new PigTest("src/test/resources/pigprocessortest.pig", args);
        test.assertOutput("activities", input, "result", output);

    }
}