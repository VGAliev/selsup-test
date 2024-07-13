import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.net.ssl.HttpsURLConnection;

public class CrptApi {

    private static final String CRPT_API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final Semaphore semaphore;
    private final AtomicInteger requestCount;
    private final ObjectMapper objectMapper;
    private final int requestLimit;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.semaphore = new Semaphore(requestLimit);
        this.requestCount = new AtomicInteger(0);
        this.objectMapper = new ObjectMapper();
        this.requestLimit = requestLimit;
        startRequestCountResetTask(timeUnit);
    }

    private void startRequestCountResetTask(TimeUnit timeUnit) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        long delay = convertToMillis(timeUnit);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            requestCount.set(0);
            semaphore.release(requestLimit - semaphore.availablePermits());
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    private long convertToMillis(TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECONDS -> {
                return 1000;
            }
            case MINUTES -> {
                return 60 * 1000;
            }
            case HOURS -> {
                return 60 * 60 * 1000;
            }
            case DAYS -> {
                return 24 * 60 * 60 * 1000;
            }
            default -> throw new IllegalArgumentException("Unsupported TimeUnite: " + timeUnit);
        }
    }

    /**
     * Создание документа для ввода в оборот товара, произведенного в РФ
     * @param document  - документ
     * @param signature - подпись
     * @return созданный документ
     */
    public String createDocumentGoodsRF(Document document, String signature) throws InterruptedException, IOException {
        semaphore.acquire();
        try {
            synchronized (requestCount) {
                if (requestCount.incrementAndGet() > requestLimit) {
                    requestCount.decrementAndGet();
                    Thread.sleep(convertToMillis(TimeUnit.SECONDS));  // wait for a second before retrying
                    return createDocumentGoodsRF(document, signature);
                }
            }
            return sendPostRequest(document, signature);
        } finally {
            semaphore.release();
        }
    }

    private String sendPostRequest(Object document, String signature) throws IOException {
        HttpsURLConnection connection = (HttpsURLConnection) new URL(CRPT_API_URL).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Signature", signature);
        connection.setDoOutput(true);

        String jsonBody = objectMapper.writeValueAsString(document);
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonBody.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpsURLConnection.HTTP_OK) {
            try (var in = new java.util.Scanner(connection.getInputStream(), StandardCharsets.UTF_8)) {
                return in.useDelimiter("\\A").next();
            }
        } else {
            throw new IOException("Error response from API: " + responseCode);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Document {
        @JsonProperty("description")
        public Description description;
        @JsonProperty("doc_id")
        public String docId;
        @JsonProperty("doc_status")
        public String docStatus;
        @JsonProperty("doc_type")
        public String docType = "LP_INTRODUCE_GOODS";
        @JsonProperty("importRequest")
        public boolean importRequest;
        @JsonProperty("owner_inn")
        public String ownerInn;
        @JsonProperty("participant_inn")
        public String participantInn;
        @JsonProperty("producer_inn")
        public String producerInn;
        @JsonProperty("production_date")
        public String productionDate;
        @JsonProperty("production_type")
        public String productionType;
        @JsonProperty("products")
        public Product[] products;
        @JsonProperty("reg_date")
        public String regDate;
        @JsonProperty("reg_number")
        public String regNumber;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Description {
            @JsonProperty("participantInn")
            public String participantInn;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Product {
            @JsonProperty("certificate_document")
            public String certificateDocument;
            @JsonProperty("certificate_document_date")
            public String certificateDocumentDate;
            @JsonProperty("certificate_document_number")
            public String certificateDocumentNumber;
            @JsonProperty("owner_inn")
            public String ownerInn;
            @JsonProperty("producer_inn")
            public String producerInn;
            @JsonProperty("production_date")
            public String productionDate;
            @JsonProperty("tnved_code")
            public String tnvedCode;
            @JsonProperty("uit_code")
            public String uitCode;
            @JsonProperty("uitu_code")
            public String uituCode;
        }
    }

    public static void main(String[] args) {
        CrptApi client = new CrptApi(TimeUnit.MINUTES, 5);
        Document document = new Document();
        document.description = new Document.Description();
        document.description.participantInn = "1234567890";
        document.docId = "123";
        document.docStatus = "DRAFT";
        document.importRequest = true;
        document.ownerInn = "0987654321";
        document.participantInn = "1234567890";
        document.producerInn = "1234567890";
        document.productionDate = "2020-01-23";
        document.productionType = "type";
        document.products = new Document.Product[1];
        document.products[0] = new Document.Product();
        document.products[0].certificateDocument = "certificate";
        document.products[0].certificateDocumentDate = "2020-01-23";
        document.products[0].certificateDocumentNumber = "123";
        document.products[0].ownerInn = "0987654321";
        document.products[0].producerInn = "1234567890";
        document.products[0].productionDate = "2020-01-23";
        document.products[0].tnvedCode = "tnved";
        document.products[0].uitCode = "uit";
        document.products[0].uituCode = "uitu";
        document.regDate = "2020-01-23";
        document.regNumber = "123";

        Runnable task = () -> {
            try {
                String response = client.createDocumentGoodsRF(document, "signature");
                System.out.println("Response: " + response);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        };

        // Запуск нескольких потоков для тестирования
        for (int i = 0; i < 10; i++) {
            new Thread(task).start();
        }
    }
}