package org.tikv.cdc.kv;

import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.model.PolymorphicEvent;

/** Event listener for CDC events and asynchronous client failures. */
public interface EventListener {
    void notify(PolymorphicEvent event);

    default void onException(ClientException exception) {}
}
