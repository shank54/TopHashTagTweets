package tweets.storm.tools;
import java.util.*;

public class ContainsHashTags{

	public static Set<String> getHashtags(String tweet) {

        Set<String> hashtags = new HashSet<>();
        int found = 1;
        int index = 0;

        while (found > 0) {
            found = tweet.indexOf("#", index);
            if (found > 0) {
                int end = tweet.indexOf(" ", found + 1);
                if (end == -1) {
                    end = tweet.length();
                }
                String hashtag = tweet.substring(found, end);
                index = end;
                hashtags.add(hashtag);
            }
        }
        return hashtags;
    }
}