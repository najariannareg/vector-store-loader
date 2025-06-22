package com.example.vectorstoreloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
public class VectorStoreLoaderApplication {

	private static final Logger logger = LoggerFactory.getLogger(VectorStoreLoaderApplication.class);

	@Value("classpath:prompts/name-of-the-game.st")
	private Resource nameOfTheGameTemplateResource;

	public static void main(String[] args) {
		SpringApplication.run(VectorStoreLoaderApplication.class, args);
	}

	@Bean
	Function<Flux<byte[]>, Flux<Document>> documentReader() {
		return resourceFlux -> resourceFlux
				.map(fileBytes -> new TikaDocumentReader(new ByteArrayResource(fileBytes)).get().getFirst())
				.subscribeOn(Schedulers.boundedElastic());
	}

	@Bean
	Function<Flux<Document>, Flux<List<Document>>> splitter() {
		return documentFlux -> documentFlux
				.map(incoming -> new TokenTextSplitter()
						.apply(List.of(incoming))).subscribeOn(Schedulers.boundedElastic());
	}

	@Bean
	Function<Flux<List<Document>>, Flux<List<Document>>> titleDeterminer(ChatClient.Builder chatClientBuilder) {
		ChatClient chatClient = chatClientBuilder.build();

		return documentListFlux -> documentListFlux
				.map(documents -> {
					if (!documents.isEmpty()) {
						Document firstDocument = documents.getFirst();

						GameTitle gameTitle = chatClient.prompt()
								.user(userSpec -> userSpec
										.text(nameOfTheGameTemplateResource)
										.param("document", Objects.requireNonNull(firstDocument.getText())))
								.call()
								.entity(GameTitle.class);

						if (Objects.requireNonNull(gameTitle).title().equals("UNKNOWN")) {
							logger.warn("Unable to determine the name of a game; not adding to vector store.");
							documents = Collections.emptyList();
							return documents;
						}

						logger.info("Determined game title to be {}", gameTitle.title());
						documents = documents.stream().peek(document -> {
							document.getMetadata().put("gameTitle", gameTitle.getNormalizedTitle());
						}).toList();
					}
					return documents;
				});
	}

	@Bean
	Consumer<Flux<List<Document>>> vectorStoreConsumer(VectorStore vectorStore) {
		return documentFlux -> documentFlux
				.doOnNext(documents -> {
					long docCount = documents.size();
					logger.info("Writing {} documents to vector store.", docCount);
					vectorStore.accept(documents);
					logger.info("{} documents have been written to vector store.", docCount);
				}).subscribe();
	}

	@Bean
	ApplicationRunner go(FunctionCatalog catalog) {
		Runnable composedFunction = catalog.lookup(null);
		return args -> {
			logger.info("Starting vector store loader...");
			composedFunction.run();
			logger.info("Vector store loading completed.");
		};
	}
}
