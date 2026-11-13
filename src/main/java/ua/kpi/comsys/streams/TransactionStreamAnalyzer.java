package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Gatherers;
import java.util.stream.Stream;

public class TransactionStreamAnalyzer {

    public Optional<Transaction> findFirstHighValueTransaction(
            Stream<Transaction> stream, BigDecimal threshold) {
        requireNonNull(stream, "stream");
        requireNonNull(threshold, "threshold");

        return stream.dropWhile(transaction -> transaction.amount().compareTo(threshold) <= 0)
                .findFirst();
    }

    public List<Transaction> takeTransactionsUntilBudgetExceeded(
            Stream<Transaction> stream, BigDecimal maxBudget) {
        requireNonNull(stream, "stream");
        requireNonNull(maxBudget, "maxBudget");

        return stream.sequential()
                .gather(Gatherers.scan(
                        () -> new RunningTotal(null, BigDecimal.ZERO),
                        (previous, transaction) -> new RunningTotal(
                                transaction, previous.total().add(transaction.amount()))))
                .takeWhile(running -> running.total().compareTo(maxBudget) <= 0)
                .map(RunningTotal::transaction)
                .toList();
    }

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }

    private record RunningTotal(Transaction transaction, BigDecimal total) {
    }
}