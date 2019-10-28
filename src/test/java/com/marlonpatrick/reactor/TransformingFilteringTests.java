package com.marlonpatrick.reactor;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TransformingFilteringTests {

	@Test
	public void collectMap() {
		Flux<String> animalFlux = Flux.just("aardvark", "elephant", "koala", "eagle", "kangaroo");
		
		Mono<Map<Character, String>> animalMapMono = animalFlux.collectMap(a -> a.charAt(0));
		
		StepVerifier.create(animalMapMono).expectNextMatches(map -> {
			return map.size() == 3 && map.get('a').equals("aardvark") && map.get('e').equals("eagle")
					&& map.get('k').equals("kangaroo");
		}).verifyComplete();
	}

	/**
	 * The same as fruitFlux.buffer() with no args.
	 */
	@Test
	public void collectList() {
		Flux<String> fruitFlux = Flux.just("apple", "orange", "banana", "kiwi", "strawberry");

		Mono<List<String>> fruitListMono = fruitFlux.collectList();

		StepVerifier.create(fruitListMono).expectNext(Arrays.asList("apple", "orange", "banana", "kiwi", "strawberry"))
				.verifyComplete();
	}

	/**
	 * <pre>
	 * So what? Buffering values from a reactive Flux into non-reactive List collections seems counterproductive. 
	 * 
	 * But when you combine buffer() with flatMap() , it enables each of the List collections to be processed in parallel: 
	 * 
	 * Flux.just( "apple", "orange", "banana", "kiwi", "strawberry")
	 * 		.buffer(3)
	 * 		.flatMap(x -> Flux.fromIterable(x).map(y -> y.toUpperCase())
	 * 			.subscribeOn(Schedulers.parallel()).log())
	 * 		.subscribe(); 
	 * 
	 * In this new example, you still buffer a Flux of five String values into a new Flux of List collections. 
	 * 
	 * But then you apply flatMap() to that Flux of List collections. This takes each List buffer and creates a new Flux 
	 * from its in parallel in individual threads.
	 * </pre>
	 */
	@Test
	public void buffer() {
		Flux<String> fruitFlux = Flux.just("apple", "orange", "banana", "kiwi", "strawberry");

		Flux<List<String>> bufferedFlux = fruitFlux.buffer(3);

		StepVerifier.create(bufferedFlux).expectNext(Arrays.asList("apple", "orange", "banana"))
				.expectNext(Arrays.asList("kiwi", "strawberry")).verifyComplete();
	}

	/**
	 * What’s important to understand about map() is that the mapping is performed
	 * synchronously, as each item is published by the source Flux .
	 * 
	 * If you want to perform the mapping asynchronously, you should consider the
	 * flatMap() operation.
	 * 
	 * The flatMap() operation requires some thought and practice to acquire full
	 * proficiency.
	 * 
	 * As shown in figure 10.17 , instead of simply mapping one object to another,
	 * as in the case of map(), flatMap() maps each object to a new Mono or Flux.
	 * The results of the Mono or Flux are flattened into a new resulting Flux.
	 * 
	 * When used along with subscribeOn() , flatMap() can unleash the asynchronous
	 * power of Reactor’s types.
	 * 
	 * Notice that flatMap() is given a lambda Function that transforms the incoming
	 * String into a Mono of type String.
	 * 
	 * A map() operation is then applied to the Mono to transform the String to a
	 * Player. If you stopped right there, the resulting Flux would carry Player
	 * objects, produced synchronously in the same order as with the map() example.
	 * 
	 * But the last thing you do with the Mono is call subscribeOn() to indicate
	 * that each subscription should take place in a parallel thread. Consequently,
	 * the mapping operations for multiple incoming String objects can be performed
	 * asynchronously and in parallel.
	 * 
	 * The upside to using flatMap() and subscribeOn() is that you can increase the
	 * throughput of the stream by splitting the work across multiple parallel
	 * threads. But because the work is being done in parallel, with no guarantees
	 * on which will finish first, there’s no way to know the order of items emitted
	 * in the resulting Flux .
	 * 
	 * Therefore, StepVerifier is only able to verify that each item emitted exists
	 * in the expected list of Player objects and that there will be three such
	 * items before the Flux completes.
	 */
	@Test
	public void flatMap() {
		Flux<Player> playerFlux = Flux.just("Michael Jordan", "Scottie Pippen", "Steve Kerr")
				.flatMap(n -> Mono.just(n).map(p -> {
					String[] split = p.split("\\s");
					return new Player(split[0], split[1]);
				}).subscribeOn(Schedulers.parallel()));

		List<Player> playerList = Arrays.asList(new Player("Michael", "Jordan"), new Player("Scottie", "Pippen"),
				new Player("Steve", "Kerr"));

		StepVerifier.create(playerFlux).expectNextMatches(p -> playerList.contains(p))
				.expectNextMatches(p -> playerList.contains(p)).expectNextMatches(p -> playerList.contains(p))
				.verifyComplete();
	}

	@Test
	public void map() {
		Flux<Player> playerFlux = Flux.just("Michael Jordan", "Scottie Pippen", "Steve Kerr").map(n -> {
			String[] split = n.split("\\s");
			return new Player(split[0], split[1]);
		});

		StepVerifier.create(playerFlux).expectNext(new Player("Michael", "Jordan"))
				.expectNext(new Player("Scottie", "Pippen")).expectNext(new Player("Steve", "Kerr")).verifyComplete();
	}

	@Test
	public void distinct() {
		Flux<String> animalFlux = Flux.just("dog", "cat", "bird", "dog", "bird", "anteater").distinct();

		StepVerifier.create(animalFlux).expectNext("dog", "cat", "bird", "anteater").verifyComplete();
	}

	@Test
	public void filter() {
		Flux<String> nationalParkFlux = Flux.just("Yellowstone", "Yosemite", "Grand Canyon", "Zion", "Grand Teton")
				.filter(np -> !np.contains(" "));

		StepVerifier.create(nationalParkFlux).expectNext("Yellowstone", "Yosemite", "Zion").verifyComplete();
	}

	@Test
	public void takeTime() {
		Flux<String> nationalParkFlux = Flux.just("Yellowstone", "Yosemite", "Grand Canyon", "Zion", "Grand Teton")
				.delayElements(Duration.ofSeconds(1)).take(Duration.ofMillis(3500));

		StepVerifier.create(nationalParkFlux).expectNext("Yellowstone", "Yosemite", "Grand Canyon").verifyComplete();
	}

	@Test
	public void takePosition() {
		Flux<String> nationalParkFlux = Flux.just("Yellowstone", "Yosemite", "Grand Canyon", "Zion", "Grand Teton")
				.take(3);

		StepVerifier.create(nationalParkFlux).expectNext("Yellowstone", "Yosemite", "Grand Canyon").verifyComplete();
	}

	@Test
	public void skipAFewSeconds() {
		Flux<String> skipFlux = Flux.just("one", "two", "skip a few", "ninety nine", "one hundred")
				.delayElements(Duration.ofSeconds(1)).skip(Duration.ofSeconds(4));

		StepVerifier.create(skipFlux).expectNext("ninety nine", "one hundred").verifyComplete();
	}

	@Test
	public void skipAFew() {
		Flux<String> skipFlux = Flux.just("one", "two", "skip a few", "ninety nine", "one hundred").skip(3);

		StepVerifier.create(skipFlux).expectNext("ninety nine", "one hundred").verifyComplete();
	}
}

class Player {
	String firstName, lastName;

	public Player(String firstName, String lastName) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Player other = (Player) obj;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		return true;
	}
}
