package org.scale7.cassandra.pelops;

public class Node {
    private final String address;
    private final IConnection.Config config;

    public Node(String address, IConnection.Config config) {
        this.address = address;
        this.config = config;
    }

    public String getAddress() {
        return address;
    }

    public IConnection.Config getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (!address.equals(node.address)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public String toString() {
        return address + ":" + config.getThriftPort();
    }
}
