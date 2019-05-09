package com.learn.flink.demo1;

/**
 * Created by kimvra on 2019-05-09
 */
public class Sale {
    //transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double

    private int transactionId;

    private int customerId;

    private int itemId;

    private double amountPaid;


    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public int getItemId() {
        return itemId;
    }

    public void setItemId(int itemId) {
        this.itemId = itemId;
    }

    public double getAmountPaid() {
        return amountPaid;
    }

    public void setAmountPaid(double amountPaid) {
        this.amountPaid = amountPaid;
    }

    @Override
    public String toString() {
        return "Sale{" +
                "transactionId=" + transactionId +
                ", customerId=" + customerId +
                ", itemId=" + itemId +
                ", amountPaid=" + amountPaid +
                '}';
    }
}
