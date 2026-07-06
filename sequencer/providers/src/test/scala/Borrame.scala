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

		val laDog: doer.LatchingTask[Animal] = doer.Covenant[Dog]().fulfill(dog)

		laDog.triggerCallbacks()(a => println(a), e => throw new Exception(e))

		val laDogMutatedToCat: doer.LatchingTask[Cat] = laDog.map(_ => cat)

		laDogMutatedToCat.triggerCallbacks()(b => println(b), e => throw new Exception(e))

		val dOne = doer.Task_apply(() => 1)
		val covenant = doer.Covenant[Int]()
		covenant.triggerCallbacks()(x => println(s"covenant completed with $x"), e => throw new Exception(e))
		covenant.fulfillWith(dOne)

		val stateUpdater: Animal => Maybe[doer.LatchingTask[Animal]] = {
				case Dog(ladrido) =>
					//					Maybe.some(doer.LatchingTask_ready(Dog(ladrido + " " + ladrido)))
					if ladrido.length < 10 then Maybe(doer.LatchingTask_ready(Dog(ladrido + " " + ladrido)))
					else Maybe.empty
				case Cat(maullido) =>
					Maybe(doer.LatchingTask_ready(Cat(maullido ++ maullido)))
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
