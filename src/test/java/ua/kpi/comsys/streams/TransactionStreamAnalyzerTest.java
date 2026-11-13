
package ua.kpi.comsys.streams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionStreamAnalyzerTest {

    private final TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

    private static Transaction tx(int id, String amount) {
        return new Transaction("TX-" + id, "books", new BigDecimal(amount),
                LocalDateTime.of(2026, 11, 1, 12, 0));
    }

    @Test
    void findsFirstTransactionStrictlyAboveThreshold() {
        Transaction first = tx(1, "100.00");
        Transaction second = tx(2, "100.01");
        Transaction third = tx(3, "500");

        assertEquals(Optional.of(second), analyzer.findFirstHighValueTransaction(
                Stream.of(first, second, third), new BigDecimal("100.00")));
    }

    @Test
    void returnsEmptyOptionalWhenNoTransactionMatches() {
        Optional<Transaction> result = analyzer.findFirstHighValueTransaction(
                Stream.of(tx(1, "10"), tx(2, "20")), new BigDecimal("50"));
        assertTrue(result.isEmpty());
        assertEquals("not found", result.map(Transaction::transactionId).orElse("not found"));
    }

    @Test
    void returnsEmptyOptionalOnEmptyStream() {
        assertTrue(analyzer.findFirstHighValueTransaction(
                Stream.empty(), BigDecimal.ONE).isEmpty());
    }

    @Test
    @Timeout(2)
    void findsFirstMatchInInfiniteStreamWithoutHanging() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> infinite = Stream.generate(() -> {
            int id = generated.incrementAndGet();
            return tx(id, String.valueOf(id));
        });

        Optional<Transaction> result = analyzer.findFirstHighValueTransaction(
                infinite, new BigDecimal("5"));
        assertEquals(Optional.of(tx(6, "6")), result);
        assertEquals(6, generated.get());
    }

    @Test
    @Timeout(2)
    void findsFirstMatchInInfiniteIterateStream() {
        Stream<Transaction> infinite = Stream.iterate(1, n -> n + 1)
                .map(n -> tx(n, String.valueOf(n * 10)));

        assertEquals(Optional.of(tx(4, "40")),
                analyzer.findFirstHighValueTransaction(infinite, new BigDecimal("30")));
    }

    @Test
    void findFirstValidatesArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.ONE));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null));
    }

    @Test
    void takesPrefixWithoutExceedingBudget() {
        List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.of(tx(1, "10"), tx(2, "20"), tx(3, "30")),
                new BigDecimal("35"));
        assertEquals(List.of(tx(1, "10"), tx(2, "20")), result);
    }

    @Test
    void includesTransactionThatReachesBudgetExactly() {
        List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.of(tx(1, "10.25"), tx(2, "19.75"), tx(3, "1")),
                new BigDecimal("30.00"));
        assertEquals(List.of(tx(1, "10.25"), tx(2, "19.75")), result);
    }

    @Test
    void doesNotSkipExpensiveTransactionToTakeLaterOne() {
        List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.of(tx(1, "10"), tx(2, "100"), tx(3, "1")),
                new BigDecimal("20"));
        assertEquals(List.of(tx(1, "10")), result);
    }

    @Test
    void returnsEmptyWhenFirstTransactionExceedsBudget() {
        assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.of(tx(1, "10")), new BigDecimal("5")).isEmpty());
    }

    @Test
    void acceptsZeroAmountWithinZeroBudget() {
        assertEquals(List.of(tx(1, "0")), analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.of(tx(1, "0"), tx(2, "1")), BigDecimal.ZERO));
    }

    @Test
    void returnsEmptyListForEmptyBudgetInput() {
        assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(
                Stream.empty(), BigDecimal.TEN).isEmpty());
    }

    @Test
    @Timeout(2)
    void stopsBudgetProcessingOfInfiniteStream() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> infinite = Stream.generate(() -> tx(generated.incrementAndGet(), "10"));

        assertEquals(List.of(tx(1, "10"), tx(2, "10"), tx(3, "10")),
                analyzer.takeTransactionsUntilBudgetExceeded(infinite, new BigDecimal("35")));
        assertEquals(4, generated.get());
    }

    @Test
    void budgetValidatesArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.TEN));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null));
    }

    @Test
    @Timeout(2)
    void gathersLimitedNumberOfBatchesFromInfiniteStream() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> infinite = Stream.generate(() -> {
            int id = generated.incrementAndGet();
            return tx(id, "5");
        });

        List<List<Transaction>> result = analyzer.processBatches(infinite, 2, 3);

        assertEquals(List.of(
                List.of(tx(1, "5"), tx(2, "5")),
                List.of(tx(3, "5"), tx(4, "5")),
                List.of(tx(5, "5"), tx(6, "5"))
        ), result);
        assertEquals(6, generated.get());
    }

    @Test
    void returnsLastPartialBatchForFiniteStream() {
        List<List<Transaction>> result = analyzer.processBatches(
                Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3"), tx(4, "4"), tx(5, "5")),
                2, 3);
        assertEquals(List.of(
                List.of(tx(1, "1"), tx(2, "2")),
                List.of(tx(3, "3"), tx(4, "4")),
                List.of(tx(5, "5"))
        ), result);
    }

    @Test
    void doesNotEmitMoreThanMaxBatchesFromFiniteStream() {
        List<List<Transaction>> result = analyzer.processBatches(
                Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3")), 1, 2);
        assertEquals(List.of(List.of(tx(1, "1")), List.of(tx(2, "2"))), result);
    }

    @Test
    void handlesEmptyAndShortFiniteStreams() {
        assertTrue(analyzer.processBatches(Stream.empty(), 3, 2).isEmpty());
        assertEquals(List.of(List.of(tx(1, "1"))), analyzer.processBatches(
                Stream.of(tx(1, "1")), 3, 2));
    }

    @Test
    void supportsSingleElementBatches() {
        assertEquals(List.of(List.of(tx(1, "1")), List.of(tx(2, "2"))),
                analyzer.processBatches(Stream.of(tx(1, "1"), tx(2, "2")), 1, 2));
    }

    @Test
    void processBatchesValidatesArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(null, 2, 2));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 0, 2));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), -1, 2));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 2, 0));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.processBatches(Stream.empty(), 2, -1));
    }

    @Test
    void returnsFirstTransactionWhenItImmediatelyExceedsThreshold() {
        Transaction first = tx(1, "11");
        assertEquals(Optional.of(first), analyzer.findFirstHighValueTransaction(
                Stream.of(first, tx(2, "100")), BigDecimal.TEN));
    }

    @Test
    void supportsNegativeAmountsAndNegativeThresholds() {
        assertEquals(Optional.of(tx(3, "-9")), analyzer.findFirstHighValueTransaction(
                Stream.of(tx(1, "-30"), tx(2, "-10"), tx(3, "-9")),
                new BigDecimal("-10")));
    }

    @Test
    void comparesDecimalAmountsWithoutRounding() {
        assertEquals(Optional.of(tx(3, "100.0001")), analyzer.findFirstHighValueTransaction(
                Stream.of(tx(1, "99.9999"), tx(2, "100.0000"), tx(3, "100.0001")),
                new BigDecimal("100")));
    }

    @Test
    void correctlyFindsZeroWithNegativeThreshold() {
        assertEquals(Optional.of(tx(2, "0")), analyzer.findFirstHighValueTransaction(
                Stream.of(tx(1, "-1"), tx(2, "0"), tx(3, "1")),
                new BigDecimal("-0.5")));
    }

    @Test
    void keepsEncounterOrderWhenFindingFirstInParallelStream() {
        assertEquals(Optional.of(tx(2, "110")), analyzer.findFirstHighValueTransaction(
                Stream.of(tx(1, "50"), tx(2, "110"), tx(3, "900")).parallel(),
                new BigDecimal("100")));
    }

    @Test
    @Timeout(2)
    void doesNotGenerateExtraTransactionsAfterFirstMatch() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> source = Stream.generate(() ->
                tx(generated.incrementAndGet(), "20"));
        Stream<Transaction> input = Stream.concat(
                Stream.of(tx(1, "10"), tx(2, "20"), tx(3, "30")), source);

        assertEquals(Optional.of(tx(3, "30")), analyzer.findFirstHighValueTransaction(
                input, new BigDecimal("25")));
        assertEquals(0, generated.get());
    }

    @Test
    @Timeout(2)
    void findsMatchAfterLongPrefixOfInfiniteStream() {
        assertEquals(Optional.of(tx(1001, "1001")),
                analyzer.findFirstHighValueTransaction(
                        Stream.iterate(1, n -> n + 1).map(n -> tx(n, Integer.toString(n))),
                        new BigDecimal("1000")));
    }

    @Test
    void budgetAccountsForRefundsAsNegativeAmounts() {
        assertEquals(List.of(tx(1, "20"), tx(2, "-10"), tx(3, "25")),
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(tx(1, "20"), tx(2, "-10"), tx(3, "25"), tx(4, "1")),
                        new BigDecimal("35")));
    }

    @Test
    void budgetUsesExactBigDecimalArithmetic() {
        assertEquals(List.of(tx(1, "0.10"), tx(2, "0.20")),
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(tx(1, "0.10"), tx(2, "0.20"), tx(3, "0.01")),
                        new BigDecimal("0.30")));
    }

    @Test
    void includesEntireStreamIfTotalNeverExceedsBudget() {
        List<Transaction> input = List.of(tx(1, "2"), tx(2, "3"), tx(3, "4"));
        assertEquals(input, analyzer.takeTransactionsUntilBudgetExceeded(
                input.stream(), BigDecimal.TEN));
    }

    @Test
    @Timeout(2)
    void budgetStopsBeforeReadingElementsBeyondFirstExceedingOne() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> input = Stream.concat(
                Stream.of(tx(1, "15"), tx(2, "50")),
                Stream.generate(() -> tx(generated.incrementAndGet(), "1")));

        assertEquals(List.of(tx(1, "15")), analyzer.takeTransactionsUntilBudgetExceeded(
                input, new BigDecimal("20")));
        assertEquals(0, generated.get());
    }

    @Test
    void budgetProcessesAnOrderedParallelStreamSequentially() {
        assertEquals(List.of(tx(1, "10"), tx(2, "15")),
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(tx(1, "10"), tx(2, "15"), tx(3, "10"), tx(4, "1"))
                                .parallel(),
                        new BigDecimal("30")));
    }

    @Test
    void handlesVeryLargeMonetaryAmountsWithoutOverflow() {
        assertEquals(List.of(tx(1, "999999999999999999999999999.99"), tx(2, "0.01")),
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.of(tx(1, "999999999999999999999999999.99"),
                                tx(2, "0.01"), tx(3, "0.01")),
                        new BigDecimal("1000000000000000000000000000.00")));
    }

    @Test
    @Timeout(2)
    void budgetStopsAnInfiniteIterateStreamAtExactBoundary() {
        assertEquals(List.of(tx(1, "1"), tx(2, "2"), tx(3, "3")),
                analyzer.takeTransactionsUntilBudgetExceeded(
                        Stream.iterate(1, n -> n + 1).map(n -> tx(n, Integer.toString(n))),
                        new BigDecimal("6")));
    }

    @Test
    void createsOnlyCompleteBatchesWhenSizeDividesInputLength() {
        assertEquals(List.of(
                        List.of(tx(1, "1"), tx(2, "2")),
                        List.of(tx(3, "3"), tx(4, "4"))),
                analyzer.processBatches(
                        Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3"), tx(4, "4")),
                        2, 10));
    }

    @Test
    @Timeout(2)
    void stopsInfiniteIterateStreamAfterOneBatch() {
        assertEquals(List.of(List.of(tx(1, "1"), tx(2, "2"), tx(3, "3"))),
                analyzer.processBatches(
                        Stream.iterate(1, n -> n + 1).map(n -> tx(n, Integer.toString(n))),
                        3, 1));
    }

    @Test
    void preservesOrderOfTransactionsFromParallelSource() {
        assertEquals(List.of(
                        List.of(tx(1, "1"), tx(2, "2")),
                        List.of(tx(3, "3"), tx(4, "4"))),
                analyzer.processBatches(
                        Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3"), tx(4, "4"))
                                .parallel(),
                        2, 2));
    }

    @Test
    @Timeout(2)
    void doesNotRequestExtraElementsAfterMaximumNumberOfBatches() {
        AtomicInteger generated = new AtomicInteger();
        Stream<Transaction> input = Stream.concat(
                Stream.of(tx(1, "1"), tx(2, "2")),
                Stream.generate(() -> tx(generated.incrementAndGet(), "99")));

        assertEquals(List.of(List.of(tx(1, "1"), tx(2, "2"))),
                analyzer.processBatches(input, 2, 1));
        assertEquals(0, generated.get());
    }

    @Test
    void returnsIndependentImmutableBatchSnapshots() {
        List<List<Transaction>> batches = analyzer.processBatches(
                Stream.of(tx(1, "1"), tx(2, "2"), tx(3, "3")), 2, 5);
        assertEquals(List.of(tx(1, "1"), tx(2, "2")), batches.get(0));
        assertEquals(List.of(tx(3, "3")), batches.get(1));
        assertThrows(UnsupportedOperationException.class,
                () -> batches.get(0).add(tx(4, "4")));
    }

    @Test
    void usesFreshBatchStateOnEveryInvocation() {
        assertEquals(List.of(List.of(tx(1, "1"))),
                analyzer.processBatches(Stream.of(tx(1, "1")), 1, 1));
        assertEquals(List.of(List.of(tx(2, "2"), tx(3, "3"))),
                analyzer.processBatches(Stream.of(tx(2, "2"), tx(3, "3")), 2, 1));
    }

    @Test
    void customGathererStopsWhenDownstreamRejectsFurtherBatches() {
        assertRejectingDownstreamStopsIntegration(
                TransactionStreamAnalyzer.batchesGatherer(2, 5));
    }

    private static <A> void assertRejectingDownstreamStopsIntegration(
            Gatherer<Transaction, A, List<Transaction>> gatherer) {
        A state = gatherer.initializer().get();
        AtomicInteger pushed = new AtomicInteger();

        Gatherer.Downstream<List<Transaction>> rejecting = batch -> {
            assertEquals(List.of(tx(1, "1"), tx(2, "2")), batch);
            pushed.incrementAndGet();
            return false;
        };

        assertTrue(gatherer.integrator().integrate(state, tx(1, "1"), rejecting));
        assertFalse(gatherer.integrator().integrate(state, tx(2, "2"), rejecting));
        assertEquals(1, pushed.get());
    }
}
