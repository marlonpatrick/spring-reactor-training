package com.marlonpatrick.reactor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LogicOperationsTests {

	@Test
	public void any() {
		Flux<String> animalFlux = Flux.just("aardvark", "elephant", "koala", "eagle", "kangaroo");
		
		Mono<Boolean> hasAMono = animalFlux.any(a -> a.contains("t"));
		
		StepVerifier.create(hasAMono).expectNext(true).verifyComplete();
		
		Mono<Boolean> hasZMono = animalFlux.any(a -> a.contains("z"));
		
		StepVerifier.create(hasZMono).expectNext(false).verifyComplete();
	}

	@Test
	public void all() {
		Flux<String> animalFlux = Flux.just("aardvark", "elephant", "koala", "eagle", "kangaroo");

		Mono<Boolean> hasAMono = animalFlux.all(a -> a.contains("a"));

		StepVerifier.create(hasAMono).expectNext(true).verifyComplete();

		Mono<Boolean> hasKMono = animalFlux.all(a -> a.contains("k"));

		StepVerifier.create(hasKMono).expectNext(false).verifyComplete();
	}
}
