package dk.ku.di.dms.vms.sdk.core.event;

public interface IPubSubService<ID,E> {

    /**
     *
     * @param identifier Internal queue identifier
     * @param element Element for insertion
     * @return
     */
    default boolean queue(ID identifier, E element){ return false; }

    /**
     *
     * @param identifier Internal queue identifier
     * @return The head element
     */
    default E dequeue( ID identifier ){ return null; }

}
