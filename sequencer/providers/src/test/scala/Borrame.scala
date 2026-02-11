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

		val laDog: doer.LatchedDuty[Animal] = doer.Covenant[Dog]().fulfill(dog)

		laDog.trigger()(a => println(a))

		val laDogMutatedToCat: doer.LatchedDuty[Cat] = laDog.map(_ => cat)

		laDogMutatedToCat.trigger()(b => println(b))

		val dOne = doer.Duty_mine(() => 1)
		val covenant = doer.Covenant[Int]()
		covenant.trigger()(x => println(s"covenant completed with $x"))
		covenant.fulfillWith(dOne)

		val stateUpdater: (Animal, Null) => Maybe[doer.LatchedDuty[Animal]] =
			(previous, _) => previous match {
				case Dog(ladrido) =>
					//					Maybe.some(doer.LatchedDuty_ready(Dog(ladrido + " " + ladrido)))
					if ladrido.length < 10 then Maybe(doer.LatchedDuty_ready(Dog(ladrido + " " + ladrido)))
					else Maybe.empty
				case Cat(maullido) =>
					Maybe(doer.LatchedDuty_ready(Cat(maullido ++ maullido)))
			}

		val fence = doer.CausalFence[Animal](dog)
		val steps =
			for {
				i <- fence.causalAnchor()
				x <- fence.advanceIf(stateUpdater)
				y <- fence.advanceIf(stateUpdater)
				z <- fence.advanceIf(stateUpdater)
			} yield (i, x, y, z)

		steps.trigger()(r => println(r))
		provider.shutdown()
	}

}
