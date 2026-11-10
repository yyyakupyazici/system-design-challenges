package io.paramix.chat.message;

public class ChatException extends RuntimeException {

    private final String code;

    public ChatException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
