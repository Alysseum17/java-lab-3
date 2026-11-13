
# Лабораторна робота №3 — Опрацювання нескінченних послідовностей даних за допомогою ледачих (lazy) операцій Java Stream API, безпечне керування наявністю значень через Optional та створення кастомних проміжних операцій за допомогою Stream Gatherers

## Опис проєкту

Проєкт реалізує сервіс аналізу транзакцій електронної комерції `TransactionStreamAnalyzer` (варіант 0). Він обробляє скінченні та нескінченні потоки транзакцій за допомогою **Java Stream API**, забезпечує пошук транзакцій за заданим порогом, накопичення суми в межах бюджету та групування транзакцій у батчі.

- **Нескінченні потоки** обробляються за допомогою ледачих операцій та короткого замикання, що дозволяє завершувати обчислення без необхідності перебирати всі елементи джерела.
- **Пошук транзакцій** реалізовано через `dropWhile()` і `findFirst()` з поверненням результату у вигляді `Optional<Transaction>`.
- **Накопичення бюджету** виконується за допомогою `Gatherers.scan()` і `takeWhile()` без зовнішнього змінюваного стану.
- **Групування транзакцій** реалізовано через власний `Gatherer.ofSequential()` із внутрішнім станом та обмеженням кількості батчів.
- **Валідація аргументів** забезпечує генерацію `IllegalArgumentException` для некоректних вхідних значень.
- **Збірка та тести** налаштовані через Gradle Kotlin DSL і Version Catalogs.

### Використані можливості Java

| Можливість | Де застосовано |
|---|---|
| `record`, `BigDecimal`, `LocalDateTime` | Модель транзакції та точне представлення грошових сум |
| `Stream.generate()`, `Stream.iterate()` | Створення нескінченних потоків для тестування |
| `dropWhile()`, `findFirst()` | Пошук першої транзакції, що перевищує заданий поріг |
| `Optional` | Безпечне представлення результату пошуку, який може бути відсутнім |
| `Gatherers.scan()`, `takeWhile()` | Накопичення суми транзакцій і зупинка при перевищенні бюджету |
| `Gatherer.ofSequential()` | Реалізація власної проміжної операції групування транзакцій |
| `Gatherer.Downstream` | Передавання сформованих батчів та обробка сигналу припинення обробки |
| `List`, `ArrayList`, `List.copyOf()` | Накопичення транзакцій і створення незалежних незмінюваних батчів |
| `IllegalArgumentException` | Обробка некоректних аргументів методів |

## Розрахунок варіанта

Залікової книжки немає, тому, згідно із завданням, використано порядковий номер студента у списку групи ІМ-42.

N = 21

```text
V = N mod 3 = 21 mod 3 = 0
```

Отже, виконано **варіант 0** — електронна комерція та стрім транзакцій.

## Структура проєкту

```text
java-lab-3/
├── build.gradle.kts
├── settings.gradle.kts
├── gradlew
├── gradlew.bat
├── gradle/
│   ├── libs.versions.toml
│   └── wrapper/
│       ├── gradle-wrapper.jar
│       └── gradle-wrapper.properties
├── README.md
└── src/
    ├── main/java/ua/kpi/comsys/streams/
    │   ├── Transaction.java
    │   └── TransactionStreamAnalyzer.java
    └── test/java/ua/kpi/comsys/streams/
        └── TransactionStreamAnalyzerTest.java
```

Усі Java-класи знаходяться в пакеті `ua.kpi.comsys.streams`.

## Формат вхідних даних

Аналізатор приймає `Stream<Transaction>`, де кожна транзакція представлена записом `Transaction`:

```java
public record Transaction(
        String transactionId,
        String category,
        BigDecimal amount,
        LocalDateTime timestamp
) {}
```

| Поле | Тип | Призначення |
|---|---|---|
| `transactionId` | `String` | Унікальний ідентифікатор транзакції |
| `category` | `String` | Категорія товарів або операції |
| `amount` | `BigDecimal` | Сума транзакції |
| `timestamp` | `LocalDateTime` | Дата й час транзакції |

Для грошових сум використовується `BigDecimal`, який забезпечує точні десяткові обчислення без похибок, характерних для `double`.

Модель не обмежує знак суми, тому від'ємні транзакції також підтримуються та можуть представляти, наприклад, повернення коштів.

Потоки можуть бути скінченними або нескінченними. Для нескінченних джерел завершення обробки забезпечується досягненням відповідної умови зупинки.

## Методи аналізатора

```java
public class TransactionStreamAnalyzer {

    public Optional<Transaction> findFirstHighValueTransaction(
            Stream<Transaction> stream, BigDecimal threshold) { ... }

    public List<Transaction> takeTransactionsUntilBudgetExceeded(
            Stream<Transaction> stream, BigDecimal maxBudget) { ... }

    public List<List<Transaction>> processBatches(
            Stream<Transaction> stream, int batchSize, int maxBatches) { ... }
}
```

### `findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold)`

- Знаходить першу транзакцію, сума якої **строго перевищує** заданий поріг `threshold`.
- Використовує `dropWhile()` для пропуску транзакцій, які не задовольняють умову.
- `findFirst()` завершує обробку одразу після знаходження першої відповідної транзакції.
- Повертає `Optional<Transaction>`, а якщо відповідної транзакції немає у скінченному потоці — `Optional.empty()`.
- Коректно працює з нескінченними потоками, якщо відповідна транзакція з'являється в них.
- Якщо `stream == null` або `threshold == null`, кидає `IllegalArgumentException`.

### `takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget)`

- Накопичує транзакції в порядку їх надходження, доки сумарна вартість не перевищує `maxBudget`.
- Використовує `Gatherers.scan()` для обчислення проміжних накопичених сум.
- Приватний `record RunningTotal` зберігає поточну транзакцію та суму після її врахування.
- `takeWhile()` зупиняє обробку на першій транзакції, яка призводить до перевищення бюджету.
- Транзакція, яка перевищила б бюджет, не включається до результату.
- Якщо накопичена сума точно дорівнює бюджету, транзакція включається.
- Обробка виконується послідовно, оскільки накопичення залежить від порядку транзакцій.
- Якщо `stream == null` або `maxBudget == null`, кидає `IllegalArgumentException`.

### `processBatches(Stream<Transaction> stream, int batchSize, int maxBatches)`

- Групує транзакції в батчі розміром `batchSize`.
- Реалізований через власний `Gatherer.ofSequential()` зі збереженням внутрішнього стану.
- Приватний клас `BatchState` містить поточний список транзакцій і кількість сформованих батчів.
- Після заповнення батча його незмінювана копія передається далі через `Downstream.push()`.
- Обробка зупиняється після формування `maxBatches` повних батчів або якщо наступний етап потоку більше не приймає результати.
- Для скінченного потоку останній неповний батч також включається до результату, якщо ліміт батчів не вичерпано.
- Для нескінченного потоку зчитується рівно `batchSize * maxBatches` елементів за звичайного завершення через ліміт батчів.
- Якщо `stream == null`, кидає `IllegalArgumentException`.
- Якщо `batchSize <= 0` або `maxBatches <= 0`, кидає `IllegalArgumentException`.

Фабрика `batchesGatherer()` має пакетний рівень доступу, що дозволяє окремо перевіряти поведінку Gatherer без розкриття приватного класу `BatchState`.


### Приклад використання

```java
TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

Stream<Transaction> transactions = Stream.iterate(1, n -> n + 1)
        .map(n -> new Transaction(
                "TX-" + n,
                "BOOKS",
                BigDecimal.valueOf(n * 10L),
                LocalDateTime.of(2026, 11, 1, 12, 0)
        ));

System.out.println(
        analyzer.findFirstHighValueTransaction(
                transactions, BigDecimal.valueOf(25))
                .map(Transaction::transactionId)
                .orElse("Not found")
);

List<Transaction> withinBudget = analyzer.takeTransactionsUntilBudgetExceeded(
        Stream.iterate(1, n -> n + 1)
                .map(n -> new Transaction(
                        "TX-" + n, "BOOKS",
                        BigDecimal.valueOf(n * 10L),
                        LocalDateTime.of(2026, 11, 1, 12, 0)
                )),
        BigDecimal.valueOf(35)
);

System.out.println(
        withinBudget.stream().map(Transaction::transactionId).toList()
);

List<List<Transaction>> batches = analyzer.processBatches(
        Stream.iterate(1, n -> n + 1)
                .map(n -> new Transaction(
                        "TX-" + n, "BOOKS",
                        BigDecimal.valueOf(n * 10L),
                        LocalDateTime.of(2026, 11, 1, 12, 0)
                )),
        2, 3
);

System.out.println(
        batches.stream()
                .map(batch -> batch.stream()
                        .map(Transaction::transactionId)
                        .toList())
                .toList()
);
```

Результат:

```text
TX-3
[TX-1, TX-2]
[[TX-1, TX-2], [TX-3, TX-4], [TX-5, TX-6]]
```

Для кожного методу створюється окремий потік, оскільки Stream у Java не можна повторно використовувати після виконання термінальної операції.


## Вимоги до середовища

- JDK **25**
- **Gradle 9.8.0** (постачається через Gradle Wrapper, окреме встановлення не потрібне)

Перевірити Java й Gradle можна командами:

```bash
java -version
./gradlew --version
```

## Збірка та запуск тестів

Для повної збірки проєкту:

```bash
./gradlew build
```

Для запуску тестів **JUnit 5**:

```bash
./gradlew clean test
```

У Windows замість `./gradlew` можна використовувати `gradlew.bat`.

Тестовий клас містить **41 модульний тест**, які перевіряють:

- пошук першої транзакції, яка строго перевищує заданий поріг;
- повернення `Optional.empty()` за відсутності результату;
- коректність порівняння `BigDecimal`, зокрема десяткових і від'ємних значень;
- накопичення бюджету, його точне досягнення та зупинку при перевищенні;
- врахування від'ємних транзакцій і точність арифметичних обчислень;
- обробку порожніх і скінченних потоків;
- збереження порядку обробки при передаванні паралельних потоків;
- формування повних і неповних батчів та обмеження їх кількості;
- незалежність внутрішнього стану Gatherer між окремими викликами;
- створення незалежних незмінюваних списків батчів;
- реакцію Gatherer на відмову `Downstream.push()` приймати подальші результати;
- перевірку `null`-аргументів і некоректних числових параметрів;
- роботу всіх трьох методів із нескінченними потоками `Stream.generate()` та `Stream.iterate()`.

У тестах нескінченних потоків використовується `@Timeout(2)` для контролю часу виконання.

Окремі перевірки з `AtomicInteger` підтверджують, що методи не зчитують зайві елементи після досягнення умови зупинки.

HTML-звіт із результатами тестування знаходиться за шляхом:

```text
build/reports/tests/test/index.html
```

У звіті відображаються кількість виконаних тестів, відсоток успішності, час виконання та помилки (якщо є).

**Скриншот результатів JUnit 5:**

<img width="1253" height="590" alt="image" src="https://github.com/user-attachments/assets/4abc208b-71ca-4236-b0f0-1f2fe0e82bf0" />

## Покриття коду

Для оцінювання покриття використовується **JaCoCo**. У конфігурації Gradle генерація звіту запускається автоматично після виконання тестів.

Звіт можна відкрити за шляхом:

```text
build/reports/jacoco/test/html/index.html
```

За потреби звіт можна згенерувати окремою командою:

```bash
./gradlew jacocoTestReport
```

Покриття оцінюється за виконаними інструкціями, гілками умов, рядками, методами та класами.

**Скриншот звіту JaCoCo:**

<img width="1075" height="206" alt="image" src="https://github.com/user-attachments/assets/344ca5f7-20df-4610-b9fb-fa42220e957e" />

## Технології

- Java 25
- Gradle 9.8.0 (Kotlin DSL, Gradle Wrapper)
- Gradle Version Catalogs (`gradle/libs.versions.toml`)
- Java Stream API та Stream Gatherers
- JUnit 5.14.4
- JaCoCo 0.8.15
