package io.paramix.notify.domain;

public enum Channel {
    SMS, EMAIL, PUSH;

    public String topicPrefix() {
        return name().toLowerCase();
    }
}
