package org.rocksdb;

public enum SpaceType {
    L2,
    IP;

    public static byte toByte(final SpaceType spaceType) {
        switch (spaceType) {
            case L2:
                return 0x00;
            case IP:
                return 0x01;
            default:
                throw new IllegalArgumentException("Unknown SpaceType: " + spaceType);
        }
    }

    public static SpaceType fromByte(final byte spaceType) {
        switch (spaceType) {
            case 0x00:
                return L2;
            case 0x01:
                return IP;
            default:
                throw new IllegalArgumentException("Unknown SpaceType: " + spaceType);
        }
    }
}
