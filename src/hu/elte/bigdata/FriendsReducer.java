package hu.elte.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FriendsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final IntWritable keyOut = new IntWritable(-1);
    private final IntWritable valueOut = new IntWritable(-1);

    private final Map<Integer, Set<Integer>> map = new HashMap<>();
    private Map<Integer, Set<Integer>> unknownUsers = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Integer currentUserId = key.get();
        unknownUsers.remove(currentUserId);

        // fill current friends
        Set<Integer> currentFriends = new HashSet<>();
        for (IntWritable friend : values) {
            Integer oppositeFriend = friend.get();
            currentFriends.add(oppositeFriend);

            // add to the opposite
            if (!map.containsKey(oppositeFriend)) {
                if (!unknownUsers.containsKey(oppositeFriend)) {
                    Set<Integer> unknownUserFriends = new HashSet<>();
                    unknownUserFriends.add(currentUserId);
                    unknownUsers.put(oppositeFriend, unknownUserFriends);
                } else {
                    unknownUsers.get(oppositeFriend).add(currentUserId);
                }
            } else if (!map.get(oppositeFriend).contains(currentUserId)) {
                map.get(oppositeFriend).add(currentUserId);
            }
        }

        // iterate through the users and add those which have mutual friends with the current user, but they don't know each other
        for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {

            // skip the same user
            if (entry.getKey().equals(currentUserId)) {
                continue;
            }

            // skip if they know each other
            Set<Integer> friends = entry.getValue();
            if (friends.contains(currentUserId)) {
                continue;
            }

            // search for mutual friends
            for (Integer currentFriend : currentFriends) {
                if (friends.contains(currentFriend)) {
                    keyOut.set(entry.getKey());
                    valueOut.set(currentUserId);
                    context.write(keyOut, valueOut);
                    context.write(valueOut, keyOut);
                    break;
                }
            }
        }

        // finally add the current user entry
        map.put(currentUserId, currentFriends);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (unknownUsers.size() == 0) {
            return;
        }

        for (Map.Entry<Integer, Set<Integer>> unknownUserEntry : unknownUsers.entrySet()) {
            // TODO code duplication, should eliminate
            // iterate through the users and add those which have mutual friends with the current user, but they don't know each other
            for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {

                // skip the same user
                if (entry.getKey().equals(unknownUserEntry.getKey())) {
                    continue;
                }

                // skip if they know each other
                Set<Integer> friends = entry.getValue();
                if (friends.contains(unknownUserEntry.getKey())) {
                    continue;
                }

                // search for mutual friends
                for (Integer currentFriend : unknownUserEntry.getValue()) {
                    if (friends.contains(currentFriend)) {
                        keyOut.set(entry.getKey());
                        valueOut.set(unknownUserEntry.getKey());
                        context.write(keyOut, valueOut);
                        context.write(valueOut, keyOut);
                        break;
                    }
                }
            }
        }
    }
}
