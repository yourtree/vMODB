package dk.ku.di.dms.vms.coordinator.server.schema.infra;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

/**
 * Writer threads must report issues to the main loop thread
 * So the next loop the main loop can take care of the problem
 */
public class Issue {

    public enum Category {
        UNREACHABLE_NODE,
        CHANNEL_CLOSED,
        CHANNEL_NOT_REGISTERED
    }

    // category of the issue
    public Category category;

    // the server on which the error was identified
    public ServerIdentifier server;

    public Issue(Category category, ServerIdentifier server) {
        this.category = category;
        this.server = server;
    }
}
