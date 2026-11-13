package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Gatherer;
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

    public List<List<Transaction>> processBatches(
            Stream<Transaction> stream, int batchSize, int maxBatches) {
        requireNonNull(stream, "stream");
        return stream.sequential().gather(batchesGatherer(batchSize, maxBatches)).toList();
    }

    static Gatherer<Transaction, ?, List<Transaction>> batchesGatherer(
            int batchSize, int maxBatches) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (maxBatches <= 0) {
            throw new IllegalArgumentException("maxBatches must be greater than 0");
        }

        return Gatherer.ofSequential(
                BatchState::new,
                (state, transaction, downstream) -> {
                    state.current.add(transaction);
                    if (state.current.size() < batchSize) {
                        return true;
                    }

                    boolean downstreamAcceptsMore = downstream.push(List.copyOf(state.current));
                    state.current.clear();
                    state.emitted++;
                    return downstreamAcceptsMore && state.emitted < maxBatches;
                },
                (state, downstream) -> {
                    if (!state.current.isEmpty()) {
                        downstream.push(List.copyOf(state.current));
                    }
                });
    }

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }

    private record RunningTotal(Transaction transaction, BigDecimal total) {
    }

    private static final class BatchState {
        private final List<Transaction> current = new ArrayList<>();
        private int emitted;
    }
}