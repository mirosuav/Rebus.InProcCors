# 13 — Transport in-proc dla Rebusa: przekazywanie przez referencję + weryfikacja serializowalności

Wtyczka do Rebusa dla modular monolith: moduły komunikują się przez bus, ale wewnątrz procesu wiadomość leci **przez referencję** zamiast przez serializację. Ekstrakcja modułu do osobnej usługi = zmiana jednej linii konfiguracji transportu, bez zmiany kodu handlerów. Serializowalność wiadomości pilnuje osobny test refleksyjny.

## Status (2026-07): tor „Pętla" — narzędzie wewnętrzne + content

**To nie jest pomysł produktowy i nie zajmuje slotu bibliotecznego.** Powstaje w systemie, który autor i tak utrzymuje i w którym ma wolne ręce; koszt portfelowy ≈ 0. Publiczna wartość to **benchmark i seria contentowa**, nie paczka na NuGet.

Warunek powrotu po slot produktowy: twardy dowód popytu spoza własnego systemu. Nie „to jest ciekawe" — dowód.

### Dlaczego to nie jest odmrożenie [09](../09-messaging-library-alternative/README.md#walidacja-i-zamknięcie-2026-07)

[Zasada z CLAUDE.md](../CLAUDE.md#decyzja-4--zamknięcie-portfela-trzy-filtry-dwa-tory-2026-07) zakazuje odmrażania bez nowego dowodu. 13 nie odmraża 09 — **jest innym obiektem**:

| Zarzut zamykający 09 | Czy dotyczy 13 |
|---|---|
| „N transportów × wersje brokerów × edge case'y sieciowe" → projekt firma, nie produkt pasywny | **Nie.** 13 nie buduje żadnego transportu do brokera. Transporty utrzymuje Rebus; 13 dokłada jeden transport in-proc |
| Dwa darmowe incumbenty do pobicia naraz (Wolverine + Rebus) | **Nie.** Rebus nie jest konkurentem, tylko platformą, do której 13 się wpina |
| Horyzont przychodu 2–3 lata | **Nie dotyczy.** 13 nie ma modelu przychodu i nie aspiruje do niego |
| Pass-by-reference to footgun bez mitygatora | **Dotyczy** — patrz [Świadomie zaakceptowane ryzyko](#świadomie-zaakceptowane-ryzyko-aliasing) |

Gdyby 13 kiedykolwiek miało wrócić jako produkt, wszystkie cztery wracają na stół razem z nim.

## Problem

Rebus serializuje wiadomości **także w transporcie in-memory** — celowo, dla wierności produkcji. W modular monolith, gdzie każdy moduł ma własny kontener DI, a bus jest wybrany właśnie po to, żeby moduł dało się później wyciąć do usługi, oznacza to podatek serializacyjny na **całej** komunikacji wewnątrzprocesowej.

Architektonicznie nie da się tego ominąć „od góry": `ITransport` Rebusa operuje na `TransportMessage`, która niesie `byte[] Body`.

## Zawór ucieczki: Wolverine ma to za darmo

Reguła z [STRATEGY](../STRATEGY.md#reguła-zaworu-ucieczki-pierwszy-krok-każdej-walidacji) każe zacząć od pytania „co klient ma dziś za darmo". Odpowiedź jest twarda i wynika ze źródeł, nie z dokumentacji:

- `Wolverine/Transports/Local/BufferedLocalQueue.cs` — **nie ma pola `IMessageSerializer` w ogóle**. Wrzuca `Envelope` (trzymający referencję na obiekt) do in-memory `Block<Envelope>`. Zero serializacji.
- `Wolverine/Transports/Local/DurableLocalQueue.cs` — **ma** `_serializer` i rzuca `ArgumentOutOfRangeException`, gdy go brak, bo persystuje do inboxa.

Czyli **pass-by-reference in-proc jest już darmowe i MIT** — dla trybu `EndpointMode.BufferedInMemory`. To zamyka drogę „to jest nowatorskie" i jest głównym powodem, dla którego 13 nie jest produktem.

Co przeżywa odjęcie Wolverine'a:
1. **Wolverine nie ma weryfikacji serializowalności.** Ta część jest tą samą klasą wartości co fosa [07](../07-automapper-source-gen-alternative/) (orzekanie o poprawności, nie generowanie kodu → przechodzi [test odporności na AI](../STRATEGY.md#test-odporności-na-ai-przekrojowe-kryterium-selekcji)).
2. **Wolverine zakłada jeden host i jeden kontener** (source-gen discovery po całej aplikacji). Izolacja DI per moduł to konfiguracja, którą Rebus obsługuje naturalnie (instancja busa per kontener, wspólna sieć in-proc), a Wolverine nie.
3. Autor już stoi na Rebusie — koszt przełączenia na Wolverine jest realny i osobny.

Żadne z tych trzech nie jest wystarczające, żeby zbudować produkt. Wszystkie trzy wystarczają, żeby rozwiązać własny problem i zmierzyć różnicę.

## Konstrukcja

**Szew: własny `ITransport` + własny `ISerializer`.** Pipeline Rebusa (retry, sagi, outbox, headers, unit of work) zostaje nietknięty — dlatego podmiana na RabbitMQ jest zmianą konfiguracji, nie kodu.

Nośnik referencji: **podklasa `TransportMessage`** niosąca `object MessageInstance`.

- `Rebus/Messages/TransportMessage.cs` to zwykła `public class`, **nie `sealed`** → dziedziczenie jest legalne.
- `ISerializer.Deserialize(TransportMessage)` dostaje tę samą instancję, którą zwrócił transport → wystarczy rzutowanie w dół.
- **Czasem życia zarządza GC.** Alternatywa (`ConcurrentDictionary<long, object>` + 8-bajtowy uchwyt w `Body`) wymagałaby sprzątania na ack/nack/retry/DLQ — każda nieobsłużona ścieżka to wyciek. Odrzucona.

Dlaczego nie da się użyć `InMemTransport` Rebusa: jego `Receive` woła `nextMessage.ToTransportMessage()`, co **tworzy nową** `TransportMessage` i gubi podklasę.

**Kolejka: `Channel<T>`.** `InMemNetwork` Rebusa stoi na `ConcurrentQueue`, a workery odpytują z backoffem. `Channel.Reader.WaitToReadAsync(cancellationToken)` czeka asynchronicznie → niższe opóźnienie i niższe CPU. Spodziewana wygrana **niezależna od serializacji** i potencjalnie od niej większa — to jest właściwy temat benchmarku.

Transport wozi **jeden tryb: `Reference`.** Bez flag, bez trybów warunkowych. Weryfikacja żyje poza transportem (niżej).

### Czego świadomie nie robimy

- **Statycznego analyzera Roslyn** serializowalności. Precedens [07](../07-automapper-source-gen-alternative/README.md#walidacja-wykonalności-2026-07): droga statyczna („Droga A") przegrywa z uruchomieniem prawdziwego pipeline'u, bo produkuje false-positives, a te kasują zaufanie. Serializowalność jest statycznie nierozstrzygalna w tych samych miejscach: właściwości typu `object`/interfejs, polimorfizm, otwarte generyki, cykle powstające z danych.
- **Wymuszania niemutowalności** wiadomości.
- **Abstrakcji nad brokerami.** To był zabójca 09.
- **Czystych `Channels` bez Rebusa** dla ruchu in-proc — tracisz retry/sagi/outbox/headers i łamiesz główne założenie („ten sam kod działa po ekstrakcji").

## Weryfikacja serializowalności: test refleksyjny

Nie jest częścią transportu. To zwykły test w suicie.

1. **Enumeracja typów z rejestracji handlerów**, nie z konwencji nazewniczej: wyciągnij `T` ze wszystkich domknięć `IHandleMessages<T>` zarejestrowanych w kontenerach modułów. To ground truth — zero listy do utrzymywania, zero polegania na sufiksie `*Command`/`*Event`. Wiadomość bez handlera i tak jest martwym kodem.
2. **Dla każdego typu: idempotencja round-tripu.** `s1 = serialize(instancja)`, `s2 = serialize(deserialize(s1))`, porównaj `s1` z `s2` **jako bajty**.

Dlaczego porównanie serializacji, a nie obiektów: rekurencyjny comparer grafów produkuje false-positives na kolejności kolekcji, precyzji `DateTime` i zmiennoprzecinkowych — czyli dokładnie ten tryb porażki, który repo odrzuciło w 07. Porównanie dwóch `byte[]` nie ma tego problemu i nie wymaga pisania comparera.

### Granica gwarancji

Nazwana wprost, bo gwarancja bez nazwanej granicy nie jest gwarancją (analogia: [mur zapytań dynamicznych w 07](../07-automapper-source-gen-alternative/README.md#dlaczego-nie-da-się-zwalidować-wszystkich-zapytań-ef)).

**Łapie:**
- typ, którego serializer nie potrafi obsłużyć (wyjątek);
- ciche gubienie danych **po stronie deserializacji** — `init`-only bez odpowiadającego parametru konstruktora, prywatny setter, właściwość interfejsu deserializowana do typu bazowego. `s2` różni się od `s1`.

**Nie łapie:**
- pól, których serializer **nigdy nie widział** (nie ma ich w `s1`, więc nie ma ich też w `s2`);
- przypadków zależnych od **wartości runtime**, nie od typu: `object Payload`, kolekcja polimorficzna. Refleksja wylicza typy — nie potrafi wymyślić, co w produkcji wyląduje w środku. To ten sam mur, który w 07 ogranicza Drogę B (`kształt zależy od argumentów, których w build-time nie znasz`);
- **aliasingu** — to własność handlerów, nie typów. Patrz niżej.

## Świadomie zaakceptowane ryzyko: aliasing

`09/README:39` nazwał to wprost: „in-memory bez serializacji = mutowanie współdzielonych obiektów przez wiele handlerów; bez dobrego mitygatora wyróżnik staje się footgunem".

**Decyzja 2026-07: nie mitygujemy mechanicznie.** Zostaje dyscyplina i code review.

Rozważone i odrzucone: tryb `Faithful` (round-trip oddający handlerowi kopię, uruchamiany w testach integracyjnych jako dowód, że żaden handler nie polega na aliasingu), source-gen deep clone, wymuszona niemutowalność.

**To był wybór, nie przeoczenie.** Konsekwencja do zapamiętania: bug aliasingu jest cichy — nic nie wybucha, dane są po prostu inne — i ujawni się dopiero w dniu ekstrakcji modułu do usługi, czyli dokładnie w dniu, dla którego cała ta konstrukcja istnieje. Jeśli kiedyś ten dzień się zbliży, mitygator wraca na stół przed ekstrakcją, nie po.

## Do zbadania

Żywa lista — 13 nie jest zamknięte.

- [ ] Czy pętla workera Rebusa poprawnie znosi `Receive` blokujące na `WaitToReadAsync` zamiast zwracać `null` przy pustej kolejce. **Pytanie do prototypu, nie do dokumentacji.** Jeśli nie — wygrana na pollingu przepada i zostaje sama serializacja
- [ ] Czy kroki pipeline'u Rebusa (deferral, forward, obsługa błędów/DLQ) **odtwarzają** instancję `TransportMessage`. Jeśli tak, podklasa ginie w locie i wraca odrzucony wariant ze słownikiem uchwytów — razem z całym sprzątaniem. To jest ryzyko wywrotki dla całej konstrukcji, sprawdzić **pierwsze**
- [ ] Izolacja DI per moduł: gdzie żyje wspólna instancja sieci in-proc, skoro każdy moduł ma własny kontener (rejestracja w kontenerze hosta i wstrzykiwanie w dół? statyczny singleton?)
- [ ] Durability: moduł chcący durable in-proc **musi** serializować — ten sam mur, o który rozbija się `DurableLocalQueue` Wolverine'a. Czy dopuszczamy tryb mieszany (część kolejek referencyjna, część durable), czy durability wyklucza się z 13
- [ ] Skąd brać instancje do testu refleksyjnego (ręczne fixture'y per typ vs generator typu AutoFixture) i co to robi z pokryciem klasy błędów zależnych od wartości
- [ ] **Benchmark (materiał contentowy):** 13 vs `Rebus.InMem` vs `Wolverine.BufferedLocalQueue` — przepustowość, opóźnienie, alokacje. Rozdzielić dwie wygrane: brak serializacji vs `Channel<T>` zamiast pollingu. Bez tego rozdzielenia benchmark nie mówi nic ciekawego

## Źródła

- `Rebus/Messages/TransportMessage.cs`, `Rebus/Serialization/ISerializer.cs`, `Rebus/Transport/ITransport.cs`, `Rebus/Transport/InMem/InMemTransport.cs` — https://github.com/rebus-org/Rebus
- `src/Wolverine/Transports/Local/BufferedLocalQueue.cs`, `DurableLocalQueue.cs` — https://github.com/JasperFx/wolverine
- https://wolverinefx.net/guide/messaging/transports/local.html
- https://github.com/rebus-org/Rebus/issues/599 — mookid8000 potwierdza in-mem transport jako bus wewnątrzprocesowy
