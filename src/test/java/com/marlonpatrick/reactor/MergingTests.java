package com.marlonpatrick.reactor;

import java.time.Duration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MergingTests {

	@Test
	public void firstFlux() {
		Flux<String> slowFlux = Flux.just("tortoise", "snail", "sloth").delaySubscription(Duration.ofMillis(100));
		
		Flux<String> fastFlux = Flux.just("hare", "cheetah", "squirrel");
		
		Flux<String> firstFlux = Flux.first(slowFlux, fastFlux);
		
		StepVerifier.create(firstFlux).expectNext("hare").expectNext("cheetah").expectNext("squirrel").verifyComplete();
	}

	@Test
	public void zipFluxesToObject() {
		Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa");

		Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apples");

		Flux<String> zippedFlux = Flux.zip(characterFlux, foodFlux, (c, f) -> c + " eats " + f);

		StepVerifier.create(zippedFlux).expectNext("Garfield eats Lasagna").expectNext("Kojak eats Lollipops")
				.expectNext("Barbossa eats Apples").verifyComplete();
	}

	@Test
	public void zipFluxes() {
		Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa");

		Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apples");

		Flux<Tuple2<String, String>> zippedFlux = Flux.zip(characterFlux, foodFlux);

		zippedFlux.subscribe(f -> System.out.println(f.getT1()));

		StepVerifier.create(zippedFlux)
				.expectNextMatches(p -> p.getT1().equals("Garfield") && p.getT2().equals("Lasagna"))
				.expectNextMatches(p -> p.getT1().equals("Kojak") && p.getT2().equals("Lollipops"))
				.expectNextMatches(p -> p.getT1().equals("Barbossa") && p.getT2().equals("Apples")).verifyComplete();
	}

	/**
	 * The order of items emitted from the merged Flux aligns with the timing of how
	 * they’re emitted from the sources.
	 * 
	 * Because both Flux objects are set to emit at regular rates, the values will
	 * be interleaved through the merged Flux, resulting in a character, followed by
	 * a food, followed by a character, and so forth.
	 * 
	 * If the timing of either Flux were to change, it’s possible that you might see
	 * two character items or two food items published one after the other.
	 */
	@Test
	public void mergeFluxes() {
		Flux<String> characterFlux = Flux.just("Garfield", "Kojak", "Barbossa").delayElements(Duration.ofMillis(500));

		Flux<String> foodFlux = Flux.just("Lasagna", "Lollipops", "Apples").delaySubscription(Duration.ofMillis(250))
				.delayElements(Duration.ofMillis(500));

		Flux<String> mergedFlux = characterFlux.mergeWith(foodFlux);

		StepVerifier.create(mergedFlux).expectNext("Garfield").expectNext("Lasagna").expectNext("Kojak")
				.expectNext("Lollipops").expectNext("Barbossa").expectNext("Apples").verifyComplete();
	}
}
