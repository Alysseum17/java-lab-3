package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Main {
    private static final LocalDateTime TIME = LocalDateTime.of(2026, 11, 12, 12, 0);

    public static void main(String[] args) {
        TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

        analyzer.findFirstHighValueTransaction(infiniteTransactions(), BigDecimal.valueOf(95))
                .ifPresentOrElse(
                        tx -> System.out.println("First above 95: " + tx.transactionId() + " (" + tx.amount() + ")"),
                        () -> System.out.println("First above 95: none"));

        analyzer.findFirstHighValueTransaction(infiniteTransactions().limit(3), BigDecimal.valueOf(1000))
                .ifPresentOrElse(
                        tx -> System.out.println("First above 1000: " + tx.transactionId()),
                        () -> System.out.println("First above 1000: none"));

        List<Transaction> withinBudget = analyzer.takeTransactionsUntilBudgetExceeded(
                infiniteTransactions(), BigDecimal.valueOf(100));
        System.out.println("Within budget 100: " + ids(withinBudget));

        List<List<Transaction>> batches = analyzer.processBatches(infiniteTransactions(), 3, 2);
        batches.forEach(batch -> System.out.println("Batch: " + ids(batch)));

        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> source = Stream.generate(() -> {
            int id = generated.incrementAndGet();
            return new Transaction("G-" + id, "BOOKS", BigDecimal.TEN, TIME);
        });
        analyzer.processBatches(source, 2, 2);
        System.out.println("Generated for 2 batches of 2: " + generated.get());

        try {
            analyzer.processBatches(infiniteTransactions(), 0, 1);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid batch size: " + e.getMessage());
        }
    }
    
    private static Stream<Transaction> infiniteTransactions() {
        return Stream.iterate(1, n -> n + 1)
                .map(n -> new Transaction("TX-" + n, "BOOKS", BigDecimal.valueOf(n * 10L), TIME));
    }

    private static List<String> ids(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::transactionId).toList();
    }
}