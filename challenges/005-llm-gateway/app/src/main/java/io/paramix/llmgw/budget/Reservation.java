package io.paramix.llmgw.budget;

/** Bir isteğin bütçe rezervasyonu. {@code day} ekibin saat dilimindeki gün (bütçe günlük). */
public record Reservation(String id, String team, String day, String userId, long amountMicro) {
}
