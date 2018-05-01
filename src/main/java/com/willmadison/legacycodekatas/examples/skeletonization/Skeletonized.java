package com.willmadison.legacycodekatas.examples.skeletonization;

import com.willmadison.legacycodekatas.fulfillment.warehouse.Message;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Skeletonized {
    private static final int MAX_BACKGROUND_WORKERS = 10;

    // START SNIPPET OMIT
    public Collection<Collection<Message>> batchMessages(Collection<Message> messages) {
        if (hasEnoughMessagesForBatching(messages)) {
            return partition(messages);
        }
        return Collections.singleton(messages);
    }

    private boolean hasEnoughMessagesForBatching(Collection<Message> messages) {
        return !CollectionUtils.isEmpty(messages);
    }

    private Collection<Collection<Message>> partition(Collection<Message> messages) {
        Collection<Collection<Message>> messageBatches = new ArrayList<>();

        List<Message> messagesToPartition = new ArrayList<>(messages);

        int numMessages = messagesToPartition.size();
        int batchSize = numMessages / MAX_BACKGROUND_WORKERS;

        if (batchSize > 1) {
            int from = 0;
            int to = from + batchSize;

            while (from < numMessages) {
                if (to > numMessages) {
                    to = numMessages;
                }

                messageBatches.add(messagesToPartition.subList(from, to));

                from += batchSize;
                to += batchSize;
            }
        } else {
            messageBatches.add(messages);
        }

        return messageBatches;
    }
    // END SNIPPET OMIT
}
