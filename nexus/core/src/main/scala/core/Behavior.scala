package readren.nexus
package core

trait Behavior[-A] {
	def handle(message: A): HandleResult[A]
}




