package com.example.kafka.payload;

import java.math.BigDecimal;
import java.util.Objects;

public class Transaction {

    private long userId;
    private String transactionId;
    private BigDecimal amount;

    public Transaction() {
    }

    public Transaction(long userId, String transactionId, BigDecimal amount) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.amount = amount;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return userId == that.userId && Objects.equals(transactionId, that.transactionId) && Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, transactionId, amount);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "userId=" + userId +
                ", transactionId='" + transactionId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
