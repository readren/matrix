package readren.sequencer

import providers.StandardSchedulingDp

import readren.common.Maybe

object Borrame {


	trait Animal

	case class Dog(ladrido: String) extends Animal {
		def ladrar(): String = ladrido
	}

	case class Cat(maullido: String) extends Animal {
		def maullar(): String = maullido
	}

	@main def borrameRun(): Unit = {
		val provider = new StandardSchedulingDp.Impl()
		val doer: Doer = provider.provide("pepe")

		val x = doer.isInSequence
		println(x)

		val dog = Dog("buaw")
		val cat = Cat("miau")

		val laDog: doer.Capture[Animal] = doer.Captor[Dog]().capture(dog)

		laDog.triggerCallbacks()(a => println(a), e => throw new Exception(e))

		val laDogMutatedToCat: doer.Capture[Cat] = laDog.map(_ => cat)

		laDogMutatedToCat.triggerCallbacks()(b => println(b), e => throw new Exception(e))

		val dOne = doer.Task_apply(() => 1)
		val captor = doer.Captor[Int]()
		captor.triggerCallbacks()(x => println(s"captor completed with $x"), e => throw new Exception(e))
		captor.seizeWith(dOne)

		val stateUpdater: Animal => Maybe[doer.Capture[Animal]] = {
				case Dog(ladrido) =>
					//					Maybe.some(doer.Keeper(Dog(ladrido + " " + ladrido)))
					if ladrido.length < 10 then Maybe(doer.Keeper(Dog(ladrido + " " + ladrido)))
					else Maybe.empty
				case Cat(maullido) =>
					Maybe(doer.Keeper(Cat(maullido ++ maullido)))
			}

		val fence = CausalFence[Animal, doer.type](doer)(dog)
		val steps =
			for {
				i <- fence.causalAnchor()
				x <- fence.advanceIf(stateUpdater)
				y <- fence.advanceIf(stateUpdater)
				z <- fence.advanceIf(stateUpdater)
			} yield (i, x, y, z)

		steps.triggerCallbacks()(r => println(r), e => throw new Exception(e))
		provider.shutdown()
	}

}
