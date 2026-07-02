package org.tikv.cdc.kv;

public enum TiDBVersion {
    V6_5("6.5.11");

    private final String version;

    TiDBVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "TiDBVersion{" + "version='" + version + '\'' + '}';
    }
}
